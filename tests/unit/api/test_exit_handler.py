"""Unit tests for app.api.exit_handler."""

from __future__ import annotations

import logging

import pytest
from pydantic import ValidationError

from app.api.exit_handler import ExitRequestPayload, process_exit_request
from app.monitoring.active_session_registry import (
    ActiveFlightSession,
    ActiveSessionRegistry,
)
from app.monitoring.state_tracker import DroneStateTracker


# ── Helpers ──────────────────────────────────────────────────────────────────


def _register_session(
    registry: ActiveSessionRegistry,
    flight_session_id: str = "flight-001",
    drone_id: str = "drone-01",
) -> ActiveFlightSession:
    session = ActiveFlightSession(
        flight_session_id=flight_session_id,
        drone_id=drone_id,
        session_source="aws",
    )
    registry.register(session)
    return session


def _add_telemetry_state(
    tracker: DroneStateTracker,
    flight_session_id: str = "flight-001",
    drone_id: str = "drone-01",
    message_count: int = 5,
) -> None:
    """Simulate telemetry by updating the tracker several times."""
    for _ in range(message_count):
        tracker.update(
            flight_session_id,
            drone_id=drone_id,
            session_source="aws",
            raw_message={},
            parsed_payload={},
            mission_status="on_mission",
            mode="AUTO",
            position=None,
            last_seen="2026-01-01T12:00:00Z",
        )


# ── ExitRequestPayload validation ────────────────────────────────────────────


class TestExitRequestPayload:
    def test_minimal(self):
        payload = ExitRequestPayload(flight_session_id="flight-001")

        assert payload.flight_session_id == "flight-001"
        assert payload.reason is None
        assert payload.requested_at is None

    def test_full(self):
        payload = ExitRequestPayload(
            flight_session_id="flight-001",
            reason="drone_left_early",
            requested_at="2026-04-21T19:00:00Z",
        )

        assert payload.flight_session_id == "flight-001"
        assert payload.reason == "drone_left_early"
        assert payload.requested_at == "2026-04-21T19:00:00Z"

    def test_missing_flight_session_id_raises(self):
        with pytest.raises(ValidationError):
            ExitRequestPayload()


# ── process_exit_request(): session found (accepted) ─────────────────────────


class TestExitAccepted:
    def test_accepted_for_registered_session(self):
        reg = ActiveSessionRegistry()
        tracker = DroneStateTracker()
        _register_session(reg, "flight-001", "drone-01")

        payload = ExitRequestPayload(flight_session_id="flight-001")
        result = process_exit_request(payload, reg, tracker)

        assert result["action"] == "accepted"
        assert result["flight_session_id"] == "flight-001"
        assert result["drone_id"] == "drone-01"

    def test_session_stays_in_registry(self):
        reg = ActiveSessionRegistry()
        tracker = DroneStateTracker()
        _register_session(reg, "flight-001", "drone-01")

        payload = ExitRequestPayload(flight_session_id="flight-001")
        process_exit_request(payload, reg, tracker)

        # Session must still be active — grace period handles removal.
        assert reg.get_by_flight_session_id("flight-001") is not None
        assert reg.get_by_drone_id("drone-01") is not None

    def test_state_stays_in_tracker(self):
        reg = ActiveSessionRegistry()
        tracker = DroneStateTracker()
        _register_session(reg, "flight-001", "drone-01")
        _add_telemetry_state(tracker, "flight-001", "drone-01", message_count=5)

        payload = ExitRequestPayload(flight_session_id="flight-001")
        process_exit_request(payload, reg, tracker)

        # Telemetry state must still be present — grace period handles removal.
        state = tracker.get("flight-001")
        assert state is not None
        assert state.message_count == 5

    def test_accepted_with_no_telemetry_state(self):
        reg = ActiveSessionRegistry()
        tracker = DroneStateTracker()
        _register_session(reg, "flight-001", "drone-01")
        # No telemetry added to tracker.

        payload = ExitRequestPayload(flight_session_id="flight-001")
        result = process_exit_request(payload, reg, tracker)

        assert result["action"] == "accepted"

    def test_accepted_returns_drone_id_from_session(self):
        reg = ActiveSessionRegistry()
        tracker = DroneStateTracker()
        _register_session(reg, "flight-001", "drone-from-session")

        payload = ExitRequestPayload(flight_session_id="flight-001")
        result = process_exit_request(payload, reg, tracker)

        assert result["drone_id"] == "drone-from-session"


# ── process_exit_request(): session not found ────────────────────────────────


class TestExitNotFound:
    def test_unknown_session(self):
        reg = ActiveSessionRegistry()
        tracker = DroneStateTracker()

        payload = ExitRequestPayload(flight_session_id="nonexistent")
        result = process_exit_request(payload, reg, tracker)

        assert result["action"] == "not_found"
        assert result["flight_session_id"] == "nonexistent"

    def test_completed_session(self):
        reg = ActiveSessionRegistry()
        tracker = DroneStateTracker()
        _register_session(reg, "flight-001", "drone-01")

        # Simulate worker finalizing the session.
        reg.complete("flight-001")

        payload = ExitRequestPayload(flight_session_id="flight-001")
        result = process_exit_request(payload, reg, tracker)

        assert result["action"] == "not_found"


# ── process_exit_request(): reason handling ──────────────────────────────────


class TestExitReason:
    def test_reason_passed_through(self, caplog):
        reg = ActiveSessionRegistry()
        tracker = DroneStateTracker()
        _register_session(reg, "flight-001", "drone-01")

        payload = ExitRequestPayload(
            flight_session_id="flight-001",
            reason="drone_left_early",
        )

        with caplog.at_level(logging.INFO, logger="app.api.exit_handler"):
            process_exit_request(payload, reg, tracker)

        assert "drone_left_early" in caplog.text

    def test_reason_defaults_to_unspecified(self, caplog):
        reg = ActiveSessionRegistry()
        tracker = DroneStateTracker()
        _register_session(reg, "flight-001", "drone-01")

        payload = ExitRequestPayload(flight_session_id="flight-001")

        with caplog.at_level(logging.INFO, logger="app.api.exit_handler"):
            process_exit_request(payload, reg, tracker)

        assert "unspecified" in caplog.text


# ── process_exit_request(): logging ──────────────────────────────────────────


class TestExitLogging:
    def test_accepted_logs_info(self, caplog):
        reg = ActiveSessionRegistry()
        tracker = DroneStateTracker()
        _register_session(reg, "flight-001", "drone-01")
        _add_telemetry_state(tracker, "flight-001", "drone-01", message_count=7)

        payload = ExitRequestPayload(
            flight_session_id="flight-001",
            reason="drone_left_early",
        )

        with caplog.at_level(logging.INFO, logger="app.api.exit_handler"):
            process_exit_request(payload, reg, tracker)

        assert "Exit request accepted" in caplog.text
        assert "flight-001" in caplog.text
        assert "drone-01" in caplog.text
        assert "drone_left_early" in caplog.text
        assert "telemetry_messages_so_far=7" in caplog.text

    def test_not_found_logs_info(self, caplog):
        reg = ActiveSessionRegistry()
        tracker = DroneStateTracker()

        payload = ExitRequestPayload(
            flight_session_id="nonexistent",
            reason="drone_left_early",
        )

        with caplog.at_level(logging.INFO, logger="app.api.exit_handler"):
            process_exit_request(payload, reg, tracker)

        assert "unknown or already-completed" in caplog.text
        assert "nonexistent" in caplog.text
