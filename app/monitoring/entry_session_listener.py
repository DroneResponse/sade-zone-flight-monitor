"""Minimal listener for AWS-approved entry sessions.

This module is the bridge between SADE's entry workflow and the local telemetry
tracker. It does not call AWS directly yet. Instead, it focuses on:
- retaining request context from the original entry submission/receipt
- parsing authoritative entry status responses
- registering approved sessions in ``ActiveSessionRegistry`` once a
  ``flight_session_id`` exists

Design note:
The AWS docs make ``GET /entry-requests/{evaluation_series_id}`` the source of
truth. The initial ``POST /entry-request`` receipt is useful for correlation,
but it is not enough to start telemetry tracking.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.monitoring.active_session_registry import ActiveFlightSession, ActiveSessionRegistry

APPROVED_DECISIONS = {"APPROVED", "APPROVED_CONSTRAINTS"}


@dataclass
class EntryRequestContext:
    """Stored correlation data from the original entry request flow."""

    evaluation_series_id: str
    drone_id: str | None = None
    pilot_id: str | None = None
    organization_id: str | None = None
    sade_zone_id: str | None = None
    requested_entry_time: str | None = None
    requested_exit_time: str | None = None
    status_url: str | None = None
    notification_topic: str | None = None
    request_payload: dict[str, Any] = field(default_factory=dict)
    receipt_payload: dict[str, Any] = field(default_factory=dict)
    stored_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class EntrySessionListener:
    """Collect approved entry-session information and register active sessions."""

    def __init__(self, session_registry: ActiveSessionRegistry | None = None) -> None:
        self.session_registry = session_registry or ActiveSessionRegistry()
        self._contexts_by_evaluation_series_id: dict[str, EntryRequestContext] = {}

    def store_request_context(
        self,
        entry_request_payload: dict[str, Any],
        acceptance_receipt: dict[str, Any],
    ) -> EntryRequestContext:
        """Store request/receipt correlation data from ``POST /entry-request``.

        This is the minimal information the tracker side should keep while it
        waits for the authoritative status resource to reach an approved state.
        """
        evaluation_series_id = acceptance_receipt.get("evaluation_series_id")
        if not isinstance(evaluation_series_id, str) or not evaluation_series_id:
            raise ValueError("Acceptance receipt is missing evaluation_series_id")

        notifications = acceptance_receipt.get("notifications") or {}
        entry_updates = notifications.get("entry_request_updates") if isinstance(notifications, dict) else {}

        context = EntryRequestContext(
            evaluation_series_id=evaluation_series_id,
            drone_id=_optional_str(entry_request_payload.get("drone_id")),
            pilot_id=_optional_str(entry_request_payload.get("pilot_id")),
            organization_id=_optional_str(entry_request_payload.get("organization_id")),
            sade_zone_id=_optional_str(entry_request_payload.get("sade_zone_id")),
            requested_entry_time=_optional_str(entry_request_payload.get("requested_entry_time")),
            requested_exit_time=_optional_str(entry_request_payload.get("requested_exit_time")),
            status_url=_optional_str(acceptance_receipt.get("status_url")),
            notification_topic=_optional_str(entry_updates.get("topic") if isinstance(entry_updates, dict) else None),
            request_payload=dict(entry_request_payload),
            receipt_payload=dict(acceptance_receipt),
        )
        self._contexts_by_evaluation_series_id[evaluation_series_id] = context
        return context

    def process_entry_status_response(
        self,
        status_response: dict[str, Any],
        *,
        request_context: EntryRequestContext | None = None,
    ) -> ActiveFlightSession | None:
        """Parse one AWS entry-status response and register an approved session.

        Returns an ``ActiveFlightSession`` only when the workflow is approved and
        includes a non-empty ``flight_session_id``.
        """
        entry_request = status_response.get("entry_request")
        if not isinstance(entry_request, dict):
            return None

        evaluation_series_id = _optional_str(entry_request.get("evaluation_series_id"))
        if not evaluation_series_id:
            return None

        context = request_context or self._contexts_by_evaluation_series_id.get(evaluation_series_id)
        decision = _optional_str(entry_request.get("decision"))
        flight_session_id = _optional_str(entry_request.get("flight_session_id"))

        if decision not in APPROVED_DECISIONS:
            return None
        if not flight_session_id:
            return None

        active_session = ActiveFlightSession(
            flight_session_id=flight_session_id,
            evaluation_series_id=evaluation_series_id,
            drone_id=context.drone_id if context else None,
            pilot_id=context.pilot_id if context else None,
            organization_id=context.organization_id if context else None,
            sade_zone_id=context.sade_zone_id if context else None,
            decision=decision,
            requested_entry_time=context.requested_entry_time if context else None,
            requested_exit_time=context.requested_exit_time if context else None,
            status_url=context.status_url if context else None,
            notification_topic=context.notification_topic if context else None,
            constraints=_extract_constraints(entry_request),
            source_payload=dict(status_response),
        )
        self.session_registry.register(active_session)
        return active_session

    def get_request_context(self, evaluation_series_id: str) -> EntryRequestContext | None:
        """Return stored entry-request context for one evaluation series."""
        return self._contexts_by_evaluation_series_id.get(evaluation_series_id)

    def drop_request_context(self, evaluation_series_id: str) -> EntryRequestContext | None:
        """Remove stored entry-request context when it is no longer needed."""
        return self._contexts_by_evaluation_series_id.pop(evaluation_series_id, None)



def _extract_constraints(entry_request: dict[str, Any]) -> dict[str, Any] | None:
    """Extract a minimal constraints payload when present in approved responses."""
    constraints = entry_request.get("constraints")
    if isinstance(constraints, dict):
        return dict(constraints)
    return None



def _optional_str(value: Any) -> str | None:
    """Return a string value or None when the input is empty/invalid."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None
