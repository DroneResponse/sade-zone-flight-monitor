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


async def post_tracker_session_finalized(payload: dict[str, Any]) -> dict[str, Any] | None:
    """POST a finalization report to the SADE tracker-session-finalized endpoint.

    The call is fire-and-log: the pipeline does not block further processing on
    the response, but does log the full outcome so failures are visible.

    Returns the parsed JSON response body on HTTP 200, or None on any error.
    The API is synchronous (HTTP 200, not 202) so the response carries the
    final ``status`` (EXITED | FAILED) and ``reputation_record_id``.
    """
    data = json.dumps(payload).encode()
    request = urllib.request.Request(
        TRACKER_FINALIZED_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    LOGGER.info(
        "Sending tracker finalization POST for flight_session_id=%s actual_start=%s actual_end=%s",
        payload.get("flight_session_id"),
        payload.get("actual_start_time"),
        payload.get("actual_end_time"),
    )

    try:
        # urllib.request is blocking; run it in a thread so the asyncio event
        # loop stays responsive while the HTTP round-trip completes.
        def _do_post() -> tuple[int, dict]:
            with urllib.request.urlopen(request, timeout=10) as resp:
                return resp.status, json.loads(resp.read())

        http_status, result = await asyncio.to_thread(_do_post)

        # Log URL + HTTP status + full raw body so every integration run has a
        # complete, auditable record of what the SADE endpoint returned.
        LOGGER.info(
            "POST %s → HTTP %s | flight_session_id=%s | body=%s",
            TRACKER_FINALIZED_URL,
            http_status,
            payload.get("flight_session_id"),
            json.dumps(result),
        )
        _log_finalization_response(result)
        return result

    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        LOGGER.error(
            "POST %s → HTTP %s | flight_session_id=%s | body=%s",
            TRACKER_FINALIZED_URL,
            exc.code,
            payload.get("flight_session_id"),
            body,
        )
        return None

    except Exception as exc:
        LOGGER.error(
            "POST %s failed for flight_session_id=%s: %s",
            TRACKER_FINALIZED_URL,
            payload.get("flight_session_id"),
            exc,
        )
        return None


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
