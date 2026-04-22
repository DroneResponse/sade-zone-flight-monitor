"""Processing logic for session registration and legacy entry approval webhooks.

This module is intentionally separated from the HTTP layer (server.py) so the
core decision logic can be tested without standing up a web server.

Two payload shapes are supported:

1. ``RegisterSessionPayload`` — matches the FLIGHT_MONITOR_CONTRACT.md contract.
   Used by ``POST /flight-monitor/register-session``.  Registration implicitly
   means the session is approved; no decision field is needed.

2. ``EntryApprovalPayload`` — deprecated transitional model from the original
   entry-approval webhook.  Kept for backward compatibility.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, model_validator

# Imported from Current_System_Versions/ via the sys.path set in api/__init__.py
from app.monitoring.active_session_registry import ActiveFlightSession, ActiveSessionRegistry

LOGGER = logging.getLogger(__name__)

# Decisions that should activate telemetry tracking for a drone.
APPROVED_DECISIONS = {"APPROVED", "APPROVED_CONSTRAINTS"}


class EntryApprovalPayload(BaseModel):
    """Incoming JSON body for POST /entry-approval.

    Accepts the key fields produced by the SADE entry workflow.  Both
    ``decision`` (SADE's field name) and ``status`` (common webhook convention)
    are accepted; ``decision`` takes precedence when both are present.

    ``flight_session_id`` is required for APPROVED events — without it the
    pipeline cannot link telemetry messages to a session.
    """

    evaluation_series_id: str

    # SADE uses "decision"; webhook proxies often use "status" — accept either.
    decision: str | None = None
    status: str | None = None

    # Must be present on APPROVED events to start telemetry tracking.
    flight_session_id: str | None = None

    # Context forwarded from the original entry request.
    drone_id: str | None = None
    pilot_id: str | None = None
    organization_id: str | None = None
    sade_zone_id: str | None = None
    requested_entry_time: str | None = None
    requested_exit_time: str | None = None
    constraints: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _require_at_least_one_decision_field(self) -> "EntryApprovalPayload":
        """Ensure the payload carries some form of decision/status."""
        if not self.decision and not self.status:
            raise ValueError("Payload must include 'decision' or 'status'")
        # Normalise: store the resolved value in 'decision' for uniform access.
        if not self.decision:
            self.decision = self.status
        return self

    @property
    def resolved_decision(self) -> str:
        """Upper-cased decision string, always safe to compare."""
        return (self.decision or "").upper()


def process_approval(  # noqa: D401
    payload: EntryApprovalPayload,
    registry: ActiveSessionRegistry,
) -> dict[str, Any]:
    """Evaluate an entry approval event and register the session if approved.

    This function is the only place that mutates the registry in response to
    a webhook call.  It is synchronous and fast — no I/O, no blocking calls —
    so it is safe to call directly from an async endpoint handler.

    Returns a plain dict summarising the action taken; the caller decides how
    to serialise it into an HTTP response.
    """
    LOGGER.warning(
        "Deprecated /entry-approval endpoint called. "
        "Migrate to POST /flight-monitor/register-session. "
        "evaluation_series_id=%s drone_id=%s",
        payload.evaluation_series_id,
        payload.drone_id,
    )

    decision = payload.resolved_decision

    # ── Non-approved decisions: log and do nothing ──────────────────────────
    if decision not in APPROVED_DECISIONS:
        LOGGER.info(
            "Entry decision is not approved — skipping session registration. "
            "evaluation_series_id=%s decision=%s drone_id=%s",
            payload.evaluation_series_id,
            decision,
            payload.drone_id,
        )
        return {
            "action": "ignored",
            "reason": f"Decision '{decision}' does not activate tracking",
            "evaluation_series_id": payload.evaluation_series_id,
            "decision": decision,
        }

    # ── APPROVED but flight_session_id missing: warn and reject ─────────────
    if not payload.flight_session_id:
        LOGGER.warning(
            "APPROVED entry event is missing flight_session_id — cannot register. "
            "evaluation_series_id=%s drone_id=%s",
            payload.evaluation_series_id,
            payload.drone_id,
        )
        return {
            "action": "rejected",
            "reason": "Decision is APPROVED but flight_session_id is absent",
            "evaluation_series_id": payload.evaluation_series_id,
            "decision": decision,
        }

    # ── APPROVED with flight_session_id: register the session ───────────────
    session = ActiveFlightSession(
        flight_session_id=payload.flight_session_id,
        evaluation_series_id=payload.evaluation_series_id,
        drone_id=payload.drone_id,
        pilot_id=payload.pilot_id,
        organization_id=payload.organization_id,
        sade_zone_id=payload.sade_zone_id,
        decision=decision,
        requested_entry_time=payload.requested_entry_time,
        requested_exit_time=payload.requested_exit_time,
        constraints=payload.constraints,
        session_source="aws",
        # Store the raw webhook body for auditability / debugging.
        source_payload=payload.model_dump(),
    )

    try:
        registry.register(session)
    except ValueError as exc:
        # Registry enforces one active session per drone; surface the conflict.
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
            "evaluation_series_id": payload.evaluation_series_id,
            "flight_session_id": payload.flight_session_id,
            "decision": decision,
        }

    LOGGER.info(
        "Drone session activated via webhook. "
        "flight_session_id=%s drone_id=%s evaluation_series_id=%s decision=%s",
        session.flight_session_id,
        session.drone_id,
        session.evaluation_series_id,
        decision,
    )

    return {
        "action": "registered",
        "flight_session_id": session.flight_session_id,
        "drone_id": session.drone_id,
        "evaluation_series_id": session.evaluation_series_id,
        "decision": decision,
    }


# ── Contract-aligned session registration (FLIGHT_MONITOR_CONTRACT.md) ───────


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

    Unlike ``process_approval``, there is no decision-checking logic here.
    A call to this endpoint means the session has already been approved by
    the SADE decision workflow — we just need to start monitoring it.

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
