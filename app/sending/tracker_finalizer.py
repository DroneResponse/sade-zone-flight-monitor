"""SADE tracker session finalization.

When a drone mission is complete, the pipeline can POST one finalization
report to the SADE ``/tracker-session-finalized`` endpoint.  That call closes
out the approved session and persists final telemetry in the SADE reputation
service.

This module is intentionally separate from the CSV output path so that the two
behaviors remain independent:
  - CSV write  → always on when ``--out`` / ``row_writer`` is set (local testing)
  - API POST   → only when ``--finalize-to-api`` is set (non-local / aws mode)

Both can be active simultaneously if that is useful during integration testing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import ssl
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

from app.monitoring.state_tracker import DroneState

LOGGER = logging.getLogger(__name__)

# Target URL for the SADE /tracker-session-finalized callback.
#
# Deployments must provide this via the TRACKER_FINALIZED_URL environment
# variable so the same image can be promoted across environments (dev /
# staging / prod / new regions) without a code change.  No default is
# supplied on purpose: silently POSTing to the wrong backend is worse than
# refusing to start.  ``get_tracker_finalized_url()`` raises a clear error
# at the point of use if the variable is unset.
TRACKER_FINALIZED_URL_ENV_VAR = "TRACKER_FINALIZED_URL"


def get_tracker_finalized_url() -> str:
    """Return the configured SADE finalization URL, or raise if unset.

    Read at call time (not import time) so that tests and local runs that
    never hit the POST path can import this module without being forced to
    set the env var.  The pipeline's startup path also calls this eagerly
    when ``finalize_to_api`` is enabled so misconfiguration fails fast.
    """
    url = os.getenv(TRACKER_FINALIZED_URL_ENV_VAR)
    if not url:
        raise RuntimeError(
            f"{TRACKER_FINALIZED_URL_ENV_VAR} is not set. "
            "This environment variable is required to POST finalization reports "
            "to SADE.  Set it to the full /tracker-session-finalized URL for the "
            "target environment."
        )
    return url


# ── Outbound mTLS to SADE ─────────────────────────────────────────────────────
# The Flight Monitor presents the same "systems" identity certificate to SADE
# on outbound finalization POSTs that it presents to inbound clients on its
# own API.  API_SERVER_CERT_PATH and API_SERVER_KEY_PATH are intentionally
# reused so the operator configures one identity in one place.
#
# TRACKER_CA_CERT_PATH is independent — it's the CA used to verify SADE's
# SERVER cert during the outbound handshake, which is a different concern
# from API_CA_CERT_PATH (which verifies INBOUND CLIENT certs).  If
# TRACKER_CA_CERT_PATH is unset, the system trust store is used.


def resolve_outbound_tls_config() -> dict[str, str | None] | None:
    """Resolve outbound mTLS config from env, or None if nothing configured.

    Returns a dict shaped like
        {"ca_cert_path": ..., "client_cert_path": ..., "client_key_path": ...}
    when one or more of the three env vars is set, with None values for
    any individual entry that was left unset.  Returns None when all
    three env vars are unset (caller gets default-trust-store HTTPS).

    Raises ``RuntimeError`` when:
      - the client cert path is set but the key path isn't (or vice
        versa); a half-configured client identity is almost always a
        misconfiguration.
      - any provided path does not exist on disk; better to surface a
        wrong bind-mount at startup than as a confusing TLS handshake
        error on the first finalize POST.
    """
    cert = (os.getenv("API_SERVER_CERT_PATH") or "").strip()
    key = (os.getenv("API_SERVER_KEY_PATH") or "").strip()
    ca = (os.getenv("TRACKER_CA_CERT_PATH") or "").strip()

    if not (cert or key or ca):
        return None

    if bool(cert) != bool(key):
        raise RuntimeError(
            "Outbound mTLS is partially configured: API_SERVER_CERT_PATH "
            "and API_SERVER_KEY_PATH must be set together when presenting "
            f"a client cert to SADE.  Got cert={cert!r} key={key!r}."
        )

    missing = [p for p in (cert, key, ca) if p and not os.path.exists(p)]
    if missing:
        raise RuntimeError(
            "Outbound mTLS cert path(s) not found on disk: "
            f"{missing}.  Verify the bind-mount and path values."
        )

    return {
        "ca_cert_path": ca or None,
        "client_cert_path": cert or None,
        "client_key_path": key or None,
    }


def _build_outbound_ssl_context(
    url: str,
    tls_config: dict[str, str | None] | None,
) -> ssl.SSLContext | None:
    """Build an SSL context for an outbound POST, or None for http:// URLs.

    Verification is always strict (hostname check + CERT_REQUIRED).  A
    custom CA bundle is loaded when ``tls_config["ca_cert_path"]`` is
    set; otherwise the system trust store is used.  A client cert is
    attached when both cert and key paths are set in ``tls_config``.

    ``urlopen`` ignores the context for plain-http URLs, but we return
    None there to make the http vs. https code paths obvious to a reader.
    """
    if not url.lower().startswith("https://"):
        return None

    ca = (tls_config or {}).get("ca_cert_path")
    cert = (tls_config or {}).get("client_cert_path")
    key = (tls_config or {}).get("client_key_path")

    ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=ca)
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED

    if cert and key:
        ctx.load_cert_chain(cert, key)

    return ctx


def _build_battery_state(voltage: float | None) -> dict[str, Any]:
    """Build one ``battery_state_in`` / ``battery_state_out`` object.

    The schema carries a ``system_charge_pct`` plus a ``slots`` array of
    ``{slot_id, voltage_v}`` entries.  Current telemetry is voltage-only
    and single-slot, so this wraps one slot "A".  When firmware starts
    emitting per-slot voltages the single point of change is this helper.

    ``system_charge_pct`` is emitted as ``null`` because telemetry does
    not yet include a battery-percentage field — SADE confirmed null is
    the correct "unknown" signal until firmware (or a voltage-to-percent
    curve) provides a real value.
    """
    return {
        "system_charge_pct": None,
        "slots": [
            {
                "slot_id": "A",
                "voltage_v": float(voltage) if voltage is not None else 0.0,
            },
        ],
    }


def build_finalization_payload(state: DroneState) -> dict[str, Any]:
    """Build the POST body for ``/tracker-session-finalized`` from mission state.

    Shape per SADE_AWS_API_INFORMATION/SADE_CONTRACT.md (2026-04-22):
      - top-level: flight_session_id, report_time_utc, telemetry_summary, events
      - telemetry_summary carries only altitude_{min,max}_m + distance_flown_m
      - flown window is derived by SADE from the FLIGHT_SEGMENT events, not
        from top-level fields (those no longer exist)
      - battery state moves into each FLIGHT_SEGMENT as battery_state_{in,out}
      - EXIT_REQUEST appears in events when the operator asked to leave early
      - INCIDENT events are not emitted yet (blocked on incident-code mapping)

    Flight-segment emission has two paths:

      * Modern path — ``state.armed_field_seen`` is True (firmware emitted
        ``status.armed`` at least once during the session): emit one
        FLIGHT_SEGMENT per recorded segment in ``state.segments``.  Any
        segment still open at finalize time is auto-closed using
        ``state.last_seen`` as ``time_out_utc``.  An empty segments list
        means the drone never armed; the result is zero FLIGHT_SEGMENT
        events, which is the truthful "powered on but didn't fly" answer.

      * Legacy fallback — ``armed_field_seen`` is False (older firmware
        without an arm-state field, or the no-telemetry-ever synthetic
        case): emit one synthetic FLIGHT_SEGMENT spanning
        ``first_seen``→``last_seen`` with the session-level voltage_in /
        voltage_out.  Identical to the pre-multi-segment behaviour.
    """
    events: list[dict[str, Any]] = []

    if state.armed_field_seen:
        for seg in state.segments:
            events.append({
                "type": "FLIGHT_SEGMENT",
                "time_in_utc": _to_utc_z(seg.time_in_utc),
                # Auto-close at session.last_seen if still open.  Drones
                # that disconnect mid-flight (battery dies, antenna fails)
                # don't emit a clean disarm; this keeps the contract valid.
                "time_out_utc": _to_utc_z(seg.time_out_utc or state.last_seen),
                "battery_state_in": _build_battery_state(seg.voltage_in),
                "battery_state_out": _build_battery_state(seg.voltage_out),
            })
    else:
        events.append({
            "type": "FLIGHT_SEGMENT",
            "time_in_utc": _to_utc_z(state.first_seen),
            "time_out_utc": _to_utc_z(state.last_seen),
            "battery_state_in": _build_battery_state(state.voltage_in),
            "battery_state_out": _build_battery_state(state.voltage_out),
        })

    if state.exit_requested_at is not None:
        events.append({
            "type": "EXIT_REQUEST",
            "time_utc": _to_utc_z(state.exit_requested_at),
            "reason": state.exit_reason or "unspecified",
        })

    return {
        "flight_session_id": state.flight_session_id,
        "report_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "telemetry_summary": {
            "altitude_min_m": float(state.min_altitude) if state.min_altitude is not None else 0.0,
            "altitude_max_m": float(state.max_altitude) if state.max_altitude is not None else 0.0,
            "distance_flown_m": float(state.distance_flown_m),
        },
        "events": events,
    }


def build_stub_finalization_payload(
    flight_session_id: str,
    test_overrides: dict[str, Any],
) -> dict[str, Any]:
    """Build the POST body for ``/tracker-session-finalized`` from test overrides.

    Used when ``test_overrides`` is present on a registered session.  Override
    shape matches the new SADE contract (see SADE_CONTRACT.md §test_overrides):

        {
          "telemetry_summary": {altitude_max_m, distance_flown_m, ...},
          "events": [ {type: FLIGHT_SEGMENT, time_in_utc, time_out_utc, ...}, ... ]
        }

    Missing pieces get safe defaults so the stub payload still satisfies the
    contract.  If overrides carry no events, synthesize one FLIGHT_SEGMENT
    anchored at "now" so SADE can derive a window.
    """
    telemetry = test_overrides.get("telemetry_summary") or {}
    override_events = test_overrides.get("events")

    now_z = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if override_events:
        events = [dict(event) for event in override_events]
    else:
        events = [{
            "type": "FLIGHT_SEGMENT",
            "time_in_utc": now_z,
            "time_out_utc": now_z,
            "battery_state_in": _build_battery_state(None),
            "battery_state_out": _build_battery_state(None),
        }]

    return {
        "flight_session_id": flight_session_id,
        "report_time_utc": now_z,
        "telemetry_summary": {
            "altitude_min_m": float(telemetry.get("altitude_min_m", 0.0)),
            "altitude_max_m": float(telemetry.get("altitude_max_m", 0.0)),
            "distance_flown_m": float(telemetry.get("distance_flown_m", 0.0)),
        },
        "events": events,
    }


MAX_RETRIES = 2
RETRY_BACKOFF_SECONDS = [1.0, 2.0]


async def post_tracker_session_finalized(payload: dict[str, Any]) -> dict[str, Any] | None:
    """POST a finalization report to the SADE tracker-session-finalized endpoint.

    Retries up to ``MAX_RETRIES`` times on transient failures (network errors
    and HTTP 5xx) with exponential backoff.  HTTP 4xx errors and successful
    business-level failures (e.g. ``status=FAILED``) are not retried because
    the payload is either invalid or the session doesn't exist in SADE —
    retrying won't change the outcome.

    Retrying is safe because SADE deduplicates finalization by
    ``flight_session_id``.

    Returns the parsed JSON response body on HTTP 200, or None after all
    attempts are exhausted.
    """
    flight_session_id = payload.get("flight_session_id")
    tracker_url = get_tracker_finalized_url()

    # Pull the flown window out of the first FLIGHT_SEGMENT for the log line —
    # SADE derives the same window from events, so this is the right source.
    first_segment = next(
        (ev for ev in payload.get("events", []) if ev.get("type") == "FLIGHT_SEGMENT"),
        None,
    )
    LOGGER.info(
        "Sending tracker finalization POST for flight_session_id=%s actual_start=%s actual_end=%s",
        flight_session_id,
        (first_segment or {}).get("time_in_utc"),
        (first_segment or {}).get("time_out_utc"),
    )

    last_error: Exception | None = None

    for attempt in range(1 + MAX_RETRIES):
        try:
            result = await _attempt_post(payload, tracker_url)

            LOGGER.info(
                "POST %s → HTTP 200 | flight_session_id=%s | body=%s (attempt %d/%d)",
                tracker_url,
                flight_session_id,
                json.dumps(result),
                attempt + 1,
                1 + MAX_RETRIES,
            )
            _log_finalization_response(result)
            return result

        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            LOGGER.error(
                "POST %s → HTTP %s | flight_session_id=%s | body=%s (attempt %d/%d)",
                tracker_url,
                exc.code,
                flight_session_id,
                body,
                attempt + 1,
                1 + MAX_RETRIES,
            )

            # 4xx = client error, retrying won't help.
            if 400 <= exc.code < 500:
                return None

            # 5xx = server error, worth retrying.
            last_error = exc

        except Exception as exc:
            LOGGER.error(
                "POST %s failed for flight_session_id=%s: %s (attempt %d/%d)",
                tracker_url,
                flight_session_id,
                exc,
                attempt + 1,
                1 + MAX_RETRIES,
            )
            last_error = exc

        # Back off before the next retry (skip sleep after the last attempt).
        if attempt < MAX_RETRIES:
            delay = RETRY_BACKOFF_SECONDS[min(attempt, len(RETRY_BACKOFF_SECONDS) - 1)]
            LOGGER.info(
                "Retrying finalization for flight_session_id=%s in %.1fs",
                flight_session_id,
                delay,
            )
            await asyncio.sleep(delay)

    LOGGER.error(
        "All %d finalization attempts failed for flight_session_id=%s. Last error: %s",
        1 + MAX_RETRIES,
        flight_session_id,
        last_error,
    )
    return None


async def _attempt_post(payload: dict[str, Any], url: str) -> dict[str, Any]:
    """Execute one blocking HTTP POST in a thread and return the parsed body.

    Raises ``urllib.error.HTTPError`` on non-2xx responses and any other
    exception on network/transport failures.

    Builds an outbound SSL context per attempt so a cert rotation does not
    require a process restart; cost is a few microseconds versus the
    network round-trip dominating the call.  Returns None context for
    http:// URLs (plain HTTP, unchanged behaviour).
    """
    data = json.dumps(payload).encode()
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    tls_config = resolve_outbound_tls_config()
    ssl_context = _build_outbound_ssl_context(url, tls_config)

    def _do_post() -> dict[str, Any]:
        with urllib.request.urlopen(request, timeout=10, context=ssl_context) as resp:
            return json.loads(resp.read())

    return await asyncio.to_thread(_do_post)


def _log_finalization_response(result: dict[str, Any]) -> None:
    """Log the SADE response at the appropriate level for each business outcome.

    EXITED  → session closed and reputation record created; log at INFO.
    FAILED  → SADE accepted the request but could not finalize (e.g. session
               not found); log at WARNING so it surfaces without being fatal.
    Other   → unexpected status; log at WARNING for visibility.
    """
    status = result.get("status")
    flight_session_id = result.get("flight_session_id")
    reason = result.get("reason", "")

    if status == "EXITED":
        LOGGER.info(
            "Tracker session finalized: flight_session_id=%s reputation_record_id=%s",
            flight_session_id,
            result.get("reputation_record_id"),
        )
    elif status == "FAILED":
        LOGGER.warning(
            "Tracker finalization business failure: flight_session_id=%s reason=%s",
            flight_session_id,
            reason,
        )
    else:
        LOGGER.warning(
            "Tracker finalization returned unexpected status=%s flight_session_id=%s reason=%s",
            status,
            flight_session_id,
            reason,
        )


def _to_utc_z(iso_timestamp: str | None) -> str:
    """Normalise an ISO timestamp to the ``YYYY-MM-DDTHH:MM:SSZ`` format.

    The SADE API examples all use the trailing-Z form.  Python's isoformat()
    produces ``+00:00`` suffixes, which this helper converts.
    """
    if not iso_timestamp:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        dt = datetime.fromisoformat(iso_timestamp)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, AttributeError):
        # If the timestamp is already in an unexpected format, pass it through.
        return iso_timestamp
