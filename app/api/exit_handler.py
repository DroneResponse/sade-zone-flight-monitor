"""Processing logic for exit-request webhook events.

This module is intentionally separated from the HTTP layer (server.py) so the
core logic can be tested without standing up a web server.

Flow:
  1. SADE sends ``POST /flight-monitor/exit-request`` when a drone leaves a
     zone early or a session needs to be closed out.
  2. The handler validates the session exists and returns an acknowledgment.
  3. The session stays active so MQTT telemetry continues to be processed.
  4. The caller (server.py) schedules a background grace-period monitor that
     waits for 5 minutes of telemetry silence before finalizing.

The primary expected reason is ``drone_left_early`` — the drone departed the
zone before the authorized window ended.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel

from app.monitoring.active_session_registry import ActiveSessionRegistry
from app.monitoring.state_tracker import DroneStateTracker

LOGGER = logging.getLogger(__name__)


class ExitRequestPayload(BaseModel):
    """Incoming JSON body for ``POST /flight-monitor/exit-request``.

    ``flight_session_id`` is the only required field — it is the cross-system
    correlation key established during session registration.

    ``reason`` describes why the exit was requested.  The primary expected
    value is ``"drone_left_early"`` (drone departed the zone before the
    authorized window ended).  Other possible values may be added in the
    future (e.g. ``"window_expired"``, ``"operator_recall"``).
    """

    flight_session_id: str
    reason: str | None = None
    requested_at: str | None = None


def process_exit_request(
    payload: ExitRequestPayload,
    registry: ActiveSessionRegistry,
    state_tracker: DroneStateTracker,
) -> dict[str, Any]:
    """Validate an exit request and acknowledge it.

    The session is NOT removed here.  It stays active so the pipeline
    continues to accept MQTT telemetry during the grace period.  The caller
    is responsible for scheduling the grace-period monitor that will
    eventually finalize and clean up.

    Returns a dict describing the outcome:
    - ``action="accepted"`` when the session was found and the grace period
      should start.
    - ``action="not_found"`` when no active session exists for the given
      ``flight_session_id`` (already completed or never registered).
    """
    flight_session_id = payload.flight_session_id
    reason = payload.reason or "unspecified"
    requested_at = payload.requested_at or datetime.now(timezone.utc).isoformat()

    session = registry.get_by_flight_session_id(flight_session_id)
    if session is None:
        LOGGER.info(
            "Exit request for unknown or already-completed session. "
            "flight_session_id=%s reason=%s",
            flight_session_id,
            reason,
        )
        return {
            "action": "not_found",
            "flight_session_id": flight_session_id,
            "message": "No active session found for this flight_session_id.",
        }

    current_state = state_tracker.get(flight_session_id)

    LOGGER.info(
        "Exit request accepted — starting grace period. "
        "flight_session_id=%s drone_id=%s reason=%s "
        "requested_at=%s telemetry_messages_so_far=%s",
        flight_session_id,
        session.drone_id,
        reason,
        requested_at,
        current_state.message_count if current_state else 0,
    )

    return {
        "action": "accepted",
        "flight_session_id": flight_session_id,
        "drone_id": session.drone_id,
        "message": "Exit request accepted. Finalization report will be sent to SADE after grace period.",
    }
