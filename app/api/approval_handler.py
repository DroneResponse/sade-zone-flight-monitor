"""Pydantic model + business logic for the session registration webhook.

Separated from the HTTP layer (server.py) so the registration logic can be
tested without standing up a web server.  Matches the registration contract
in SADE_AWS_API_INFORMATION/FLIGHT_MONITOR_CONTRACT.md — a call into this
endpoint implicitly means SADE has already approved the session, so there is
no decision/status field to evaluate here.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel

from app.monitoring.active_session_registry import ActiveFlightSession, ActiveSessionRegistry

LOGGER = logging.getLogger(__name__)


class RegisterSessionPayload(BaseModel):
    """Incoming JSON body for ``POST /flight-monitor/register-session``.

    Matches the registration request contract in FLIGHT_MONITOR_CONTRACT.md.
    Registration implicitly means the session is approved — there is no
    ``decision`` field.  ``flight_session_id`` is the only required
    cross-system correlation key.
    """

    flight_session_id: str
    pilot_id: str | None = None
    drone_id: str | None = None
    organization_id: str | None = None
    sade_zone_id: str | None = None
    requested_entry_time: str | None = None
    requested_exit_time: str | None = None
    requested_operation: dict[str, Any] | None = None
    test_overrides: dict[str, Any] | None = None
    submitted_at: str | None = None


def process_register_session(
    payload: RegisterSessionPayload,
    registry: ActiveSessionRegistry,
) -> dict[str, Any]:
    """Register an approved flight session from the SADE outbox.

    There is no decision-checking logic here: a call to this endpoint means
    the session has already been approved by the SADE decision workflow —
    we just need to start monitoring it.

    Returns a plain dict summarising the action taken.
    """
    session = ActiveFlightSession(
        flight_session_id=payload.flight_session_id,
        drone_id=payload.drone_id,
        pilot_id=payload.pilot_id,
        organization_id=payload.organization_id,
        sade_zone_id=payload.sade_zone_id,
        decision="REGISTERED",
        requested_entry_time=payload.requested_entry_time,
        requested_exit_time=payload.requested_exit_time,
        requested_operation=payload.requested_operation,
        test_overrides=payload.test_overrides,
        submitted_at=payload.submitted_at,
        session_source="aws",
        source_payload=payload.model_dump(),
    )

    try:
        registry.register(session)
    except ValueError as exc:
        LOGGER.warning(
            "Session registration rejected by registry: %s "
            "flight_session_id=%s drone_id=%s",
            exc,
            payload.flight_session_id,
            payload.drone_id,
        )
        return {
            "action": "rejected",
            "reason": str(exc),
            "flight_session_id": payload.flight_session_id,
        }

    LOGGER.info(
        "Flight session registered for monitoring. "
        "flight_session_id=%s drone_id=%s",
        session.flight_session_id,
        session.drone_id,
    )

    return {
        "action": "registered",
        "flight_session_id": session.flight_session_id,
        "drone_id": session.drone_id,
    }
