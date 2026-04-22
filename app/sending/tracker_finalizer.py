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
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

from app.monitoring.state_tracker import DroneState

LOGGER = logging.getLogger(__name__)

# Authoritative URL from the SADE API reference.
# Defined here explicitly so the value stays visible and auditable.
# TODO: This should be made configurable via an environment variable
# (e.g. TRACKER_FINALIZED_URL) so deployments against different SADE
# environments don't require a code change.
TRACKER_FINALIZED_URL = (
    "http://sarec-sade-use2-api-alb-1413405053.us-east-2.elb.amazonaws.com"
    "/tracker-session-finalized"
)


def build_finalization_payload(state: DroneState) -> dict[str, Any]:
    """Build the POST body for ``/tracker-session-finalized`` from mission state.

    All fields are derived from the telemetry that the pipeline has accumulated
    in DroneState over the course of the mission.

    Field sources:
    - ``flight_session_id``     → state.flight_session_id (from SADE approval or synthetic local ID)
    - ``report_time``           → wall-clock UTC at the moment this report is sent
    - ``actual_start_time``     → state.first_seen (timestamp of the first received message)
    - ``actual_end_time``       → state.last_seen  (timestamp of the terminal status message)
    - ``altitude_min_m``        → state.min_altitude (running min accumulated from status.location.altitude)
    - ``altitude_max_m``        → state.max_altitude (running max accumulated from status.location.altitude)
    - ``battery_voltage_start_v`` → state.voltage_in  (voltage from the first message that carried it)
    - ``battery_voltage_end_v``   → state.voltage_out (voltage from the terminal message)
    - ``battery_start_pct``     → not in the telemetry schema; the drone sim and live payload both
                                   publish only voltage, not percentage. Sent as 0.0 until a
                                   percentage field is added to the telemetry payload.
    - ``battery_end_pct``       → same as above.
    """
    return {
        "flight_session_id": state.flight_session_id,
        "report_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "actual_start_time": _to_utc_z(state.first_seen),
        "actual_end_time": _to_utc_z(state.last_seen),
        "telemetry_summary": {
            "altitude_min_m": float(state.min_altitude) if state.min_altitude is not None else 0.0,
            "altitude_max_m": float(state.max_altitude) if state.max_altitude is not None else 0.0,
            "battery_start_pct": 0.0,  # voltage-only telemetry; no percentage field available
            "battery_end_pct": 0.0,    # voltage-only telemetry; no percentage field available
            "battery_voltage_start_v": float(state.voltage_in) if state.voltage_in is not None else 0.0,
            "battery_voltage_end_v": float(state.voltage_out) if state.voltage_out is not None else 0.0,
        },
    }


def build_stub_finalization_payload(
    flight_session_id: str,
    test_overrides: dict[str, Any],
) -> dict[str, Any]:
    """Build the POST body for ``/tracker-session-finalized`` from test overrides.

    Used when ``test_overrides`` is present on a registered session.  The
    override dict is expected to carry ``actual_start_time``,
    ``actual_end_time``, and an optional ``telemetry_summary`` — matching
    the shape documented in FLIGHT_MONITOR_CONTRACT.md.

    Missing fields are filled with safe defaults so the payload always
    satisfies the SADE API contract.
    """
    telemetry = test_overrides.get("telemetry_summary") or {}

    return {
        "flight_session_id": flight_session_id,
        "report_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "actual_start_time": test_overrides.get("actual_start_time")
            or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "actual_end_time": test_overrides.get("actual_end_time")
            or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "telemetry_summary": {
            "altitude_min_m": float(telemetry.get("altitude_min_m", 0.0)),
            "altitude_max_m": float(telemetry.get("altitude_max_m", 0.0)),
            "battery_start_pct": float(telemetry.get("battery_start_pct", 0.0)),
            "battery_end_pct": float(telemetry.get("battery_end_pct", 0.0)),
            "battery_voltage_start_v": float(telemetry.get("battery_voltage_start_v", 0.0)),
            "battery_voltage_end_v": float(telemetry.get("battery_voltage_end_v", 0.0)),
        },
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

    LOGGER.info(
        "Sending tracker finalization POST for flight_session_id=%s actual_start=%s actual_end=%s",
        flight_session_id,
        payload.get("actual_start_time"),
        payload.get("actual_end_time"),
    )

    last_error: Exception | None = None

    for attempt in range(1 + MAX_RETRIES):
        try:
            result = await _attempt_post(payload)

            LOGGER.info(
                "POST %s → HTTP 200 | flight_session_id=%s | body=%s (attempt %d/%d)",
                TRACKER_FINALIZED_URL,
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
                TRACKER_FINALIZED_URL,
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
                TRACKER_FINALIZED_URL,
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


async def _attempt_post(payload: dict[str, Any]) -> dict[str, Any]:
    """Execute one blocking HTTP POST in a thread and return the parsed body.

    Raises ``urllib.error.HTTPError`` on non-2xx responses and any other
    exception on network/transport failures.
    """
    data = json.dumps(payload).encode()
    request = urllib.request.Request(
        TRACKER_FINALIZED_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    def _do_post() -> dict[str, Any]:
        with urllib.request.urlopen(request, timeout=10) as resp:
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
