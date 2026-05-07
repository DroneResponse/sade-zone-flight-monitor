"""Registry of active approved or synthetic flight sessions.

This module stores the sessions that the telemetry tracker is allowed to
associate with incoming drone telemetry.

Two operating modes are supported by the wider pipeline:
1. AWS mode: sessions should come from approved SADE entry workflows.
2. Local mode: sessions may be created automatically for local testing.

Keeping both session types in one registry lets the telemetry worker share a
single lookup path while still making the mode difference explicit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


@dataclass
class ActiveFlightSession:
    """Normalized session record used by telemetry tracking.

    ``session_source`` distinguishes whether this session came from AWS approval
    or was synthesized locally for development/testing.
    """

    flight_session_id: str
    evaluation_series_id: str = ""
    drone_id: str | None = None
    pilot_id: str | None = None
    organization_id: str | None = None
    sade_zone_id: str | None = None
    decision: str | None = None
    requested_entry_time: str | None = None
    requested_exit_time: str | None = None
    status_url: str | None = None
    notification_topic: str | None = None
    session_source: str = "aws"
    constraints: dict[str, Any] | None = None
    requested_operation: dict[str, Any] | None = None
    test_overrides: dict[str, Any] | None = None
    submitted_at: str | None = None
    source_payload: dict[str, Any] = field(default_factory=dict)
    registered_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    # Set when SADE sends /flight-monitor/exit-request.  Mirrors the same
    # field on DroneState but lives on the session record so the sweeper
    # has a single source of truth even when no telemetry has arrived
    # (in which case DroneState doesn't exist yet).
    exit_requested_at: str | None = None
    # Stamped by the periodic sweeper the first time it observes that
    # ``now > requested_exit_time`` and ``exit_requested_at is None``.
    # Acts as a one-shot edge detector — once set, the session is not
    # re-flagged on subsequent sweeps.
    exit_deadline_breached_at: str | None = None


class ActiveSessionRegistry:
    """In-memory registry of active sessions.

    A single drone should have at most one active session at a time. This class
    enforces that rule while providing lookup by either ``flight_session_id`` or
    ``drone_id``.
    """

    def __init__(self) -> None:
        self._sessions_by_flight_session_id: dict[str, ActiveFlightSession] = {}
        self._flight_session_id_by_drone_id: dict[str, str] = {}

    def register(self, session: ActiveFlightSession) -> ActiveFlightSession:
        """Register or update one active session.

        Rules:
        - Re-registering the same ``flight_session_id`` updates the stored record.
        - A different session for a drone that already has an active session is rejected.
        """
        existing = self._sessions_by_flight_session_id.get(session.flight_session_id)
        if existing is not None:
            self._sessions_by_flight_session_id[session.flight_session_id] = session
            if session.drone_id:
                self._flight_session_id_by_drone_id[session.drone_id] = session.flight_session_id
            return session

        if session.drone_id:
            active_flight_session_id = self._flight_session_id_by_drone_id.get(session.drone_id)
            if active_flight_session_id is not None and active_flight_session_id != session.flight_session_id:
                raise ValueError(
                    f"Drone {session.drone_id} already has active flight session {active_flight_session_id}"
                )
            self._flight_session_id_by_drone_id[session.drone_id] = session.flight_session_id

        self._sessions_by_flight_session_id[session.flight_session_id] = session
        return session

    def ensure_local_session(self, drone_id: str) -> ActiveFlightSession:
        """Return an active local-testing session for a drone.

        This helper is used by permissive local test mode. If a local session is
        already active for the drone, it is reused; otherwise a new synthetic
        session is created and registered.
        """
        existing = self.get_by_drone_id(drone_id)
        if existing is not None:
            return existing

        synthetic_session = ActiveFlightSession(
            flight_session_id=f"local-flight-{uuid4()}",
            evaluation_series_id=f"local-eval-{uuid4()}",
            drone_id=drone_id,
            decision="LOCAL_TEST_SESSION",
            session_source="local",
            source_payload={
                "created_by": "ActiveSessionRegistry.ensure_local_session",
                "reason": "Local telemetry auto-session enabled",
            },
        )
        return self.register(synthetic_session)

    def get_by_flight_session_id(self, flight_session_id: str) -> ActiveFlightSession | None:
        """Return one active session by its flight session id."""
        return self._sessions_by_flight_session_id.get(flight_session_id)

    def get_by_drone_id(self, drone_id: str) -> ActiveFlightSession | None:
        """Return the active session for one drone, if any."""
        flight_session_id = self._flight_session_id_by_drone_id.get(drone_id)
        if flight_session_id is None:
            return None
        return self._sessions_by_flight_session_id.get(flight_session_id)

    def complete(self, flight_session_id: str) -> ActiveFlightSession | None:
        """Remove and return a completed session once tracking/finalization is done."""
        session = self._sessions_by_flight_session_id.pop(flight_session_id, None)
        if session is not None and session.drone_id is not None:
            active_flight_session_id = self._flight_session_id_by_drone_id.get(session.drone_id)
            if active_flight_session_id == flight_session_id:
                self._flight_session_id_by_drone_id.pop(session.drone_id, None)
        return session

    def snapshot(self) -> dict[str, ActiveFlightSession]:
        """Return a shallow copy of all active sessions."""
        return dict(self._sessions_by_flight_session_id)

    def count(self) -> int:
        """Return the number of active sessions currently tracked."""
        return len(self._sessions_by_flight_session_id)

    def count_past_deadline(self) -> int:
        """Return how many active sessions have been flagged past their deadline.

        Counts sessions whose ``exit_deadline_breached_at`` field has been
        stamped by the periodic sweeper.  O(N) over the active session set,
        which is expected to stay small (low tens at most) — no caching.
        """
        return sum(
            1
            for session in self._sessions_by_flight_session_id.values()
            if session.exit_deadline_breached_at is not None
        )
