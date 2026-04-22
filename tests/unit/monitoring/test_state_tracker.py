"""Unit tests for app.monitoring.state_tracker."""

from __future__ import annotations

import pytest

from app.monitoring.state_tracker import (
    DroneState,
    DroneStateTracker,
    _extract_voltage,
    _safe_float,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_position(lat: float = 39.77, lon: float = -86.16, alt: float = 100.0) -> dict:
    return {"latitude": lat, "longitude": lon, "altitude": alt}


def _make_payload(*, voltage: float | None = None, altitude: float | None = None) -> dict:
    """Build a minimal telemetry payload with optional nested voltage/altitude."""
    payload: dict = {}
    status: dict = {}
    if voltage is not None:
        status["battery"] = {"voltage": voltage}
    if altitude is not None:
        status["location"] = {"altitude": altitude}
    if status:
        payload["status"] = status
    return payload


def _do_update(
    tracker: DroneStateTracker,
    flight_session_id: str = "flight-001",
    *,
    drone_id: str = "drone-01",
    session_source: str = "local",
    mission_status: str = "on_mission",
    mode: str = "AUTO",
    position: dict | None = None,
    last_seen: str | None = "2026-01-01T00:00:00Z",
    voltage: float | None = None,
) -> DroneState:
    """Shorthand for a tracker.update() call with sensible defaults."""
    payload = _make_payload(voltage=voltage)
    return tracker.update(
        flight_session_id,
        drone_id=drone_id,
        session_source=session_source,
        raw_message={"topic": "update_drone", "payload": "..."},
        parsed_payload=payload,
        mission_status=mission_status,
        mode=mode,
        position=position,
        last_seen=last_seen,
    )


# ── update(): first message (creates new state) ─────────────────────────────


class TestFirstUpdate:
    def test_creates_state(self):
        tracker = DroneStateTracker()
        state = _do_update(tracker, "flight-001", drone_id="drone-01", session_source="aws", last_seen="2026-03-01T12:00:00Z")

        assert state.flight_session_id == "flight-001"
        assert state.drone_id == "drone-01"
        assert state.session_source == "aws"
        assert state.first_seen == "2026-03-01T12:00:00Z"
        assert state.last_seen == "2026-03-01T12:00:00Z"
        assert state.message_count == 1
        assert state.row_written is False

    def test_captures_start_position(self):
        tracker = DroneStateTracker()
        pos = _make_position(lat=39.77, lon=-86.16, alt=110.0)
        state = _do_update(tracker, position=pos)

        assert state.position == pos
        assert state.start_position == pos
        # start_position should be a copy, not the same object
        assert state.start_position is not pos

    def test_no_position(self):
        tracker = DroneStateTracker()
        state = _do_update(tracker, position=None)

        assert state.position is None
        assert state.start_position is None
        assert state.max_altitude is None
        assert state.min_altitude is None

    def test_captures_altitude(self):
        tracker = DroneStateTracker()
        pos = _make_position(alt=85.5)
        state = _do_update(tracker, position=pos)

        assert state.max_altitude == 85.5
        assert state.min_altitude == 85.5

    def test_captures_voltage(self):
        tracker = DroneStateTracker()
        state = _do_update(tracker, voltage=16.4)

        assert state.voltage_in == 16.4
        assert state.voltage_out == 16.4


# ── update(): subsequent messages (updates existing state) ───────────────────


class TestSubsequentUpdates:
    def test_increments_message_count(self):
        tracker = DroneStateTracker()
        _do_update(tracker, "flight-001")
        _do_update(tracker, "flight-001")
        state = _do_update(tracker, "flight-001")

        assert state.message_count == 3

    def test_preserves_first_seen(self):
        tracker = DroneStateTracker()
        _do_update(tracker, "flight-001", last_seen="2026-01-01T00:00:00Z")
        state = _do_update(tracker, "flight-001", last_seen="2026-01-01T00:05:00Z")

        assert state.first_seen == "2026-01-01T00:00:00Z"
        assert state.last_seen == "2026-01-01T00:05:00Z"

    def test_tracks_max_altitude(self):
        tracker = DroneStateTracker()
        _do_update(tracker, position=_make_position(alt=50.0))
        _do_update(tracker, position=_make_position(alt=90.0))
        state = _do_update(tracker, position=_make_position(alt=70.0))

        assert state.max_altitude == 90.0

    def test_tracks_min_altitude(self):
        tracker = DroneStateTracker()
        _do_update(tracker, position=_make_position(alt=80.0))
        _do_update(tracker, position=_make_position(alt=40.0))
        state = _do_update(tracker, position=_make_position(alt=60.0))

        assert state.min_altitude == 40.0

    def test_voltage_in_stays_first(self):
        tracker = DroneStateTracker()
        _do_update(tracker, voltage=16.5)
        _do_update(tracker, voltage=15.8)
        state = _do_update(tracker, voltage=15.2)

        assert state.voltage_in == 16.5

    def test_voltage_out_updates(self):
        tracker = DroneStateTracker()
        _do_update(tracker, voltage=16.5)
        _do_update(tracker, voltage=15.8)
        state = _do_update(tracker, voltage=15.2)

        assert state.voltage_out == 15.2

    def test_preserves_start_position(self):
        tracker = DroneStateTracker()
        first_pos = _make_position(lat=39.77, lon=-86.16, alt=100.0)
        second_pos = _make_position(lat=39.78, lon=-86.17, alt=110.0)

        _do_update(tracker, position=first_pos)
        state = _do_update(tracker, position=second_pos)

        assert state.start_position == first_pos
        assert state.position == second_pos

    def test_start_position_set_on_first_non_null(self):
        tracker = DroneStateTracker()
        _do_update(tracker, position=None)
        pos = _make_position(lat=39.77, lon=-86.16, alt=100.0)
        state = _do_update(tracker, position=pos)

        assert state.start_position == pos


# ── update(): edge cases ─────────────────────────────────────────────────────


class TestUpdateEdgeCases:
    def test_altitude_none_does_not_overwrite(self):
        tracker = DroneStateTracker()
        _do_update(tracker, position=_make_position(alt=75.0))
        state = _do_update(tracker, position=None)

        assert state.max_altitude == 75.0
        assert state.min_altitude == 75.0

    def test_voltage_none_does_not_overwrite(self):
        tracker = DroneStateTracker()
        _do_update(tracker, voltage=16.0)
        state = _do_update(tracker, voltage=None)

        assert state.voltage_in == 16.0
        assert state.voltage_out == 16.0

    def test_last_seen_defaults_to_utc_now(self):
        tracker = DroneStateTracker()
        state = _do_update(tracker, last_seen=None)

        # Should be a non-empty ISO timestamp string, not None
        assert isinstance(state.last_seen, str)
        assert len(state.last_seen) > 0
        assert "T" in state.last_seen


# ── get, pop, snapshot, count ────────────────────────────────────────────────


class TestTrackerLookups:
    def test_get_unknown_returns_none(self):
        tracker = DroneStateTracker()
        assert tracker.get("nonexistent") is None

    def test_get_returns_state_after_update(self):
        tracker = DroneStateTracker()
        _do_update(tracker, "flight-001", drone_id="drone-01")
        state = tracker.get("flight-001")

        assert state is not None
        assert state.drone_id == "drone-01"

    def test_pop_removes_and_returns(self):
        tracker = DroneStateTracker()
        _do_update(tracker, "flight-001")
        state = tracker.pop("flight-001")

        assert state is not None
        assert state.flight_session_id == "flight-001"
        assert tracker.get("flight-001") is None

    def test_pop_unknown_returns_none(self):
        tracker = DroneStateTracker()
        assert tracker.pop("nonexistent") is None

    def test_count_reflects_active_sessions(self):
        tracker = DroneStateTracker()
        assert tracker.count() == 0

        _do_update(tracker, "flight-001")
        assert tracker.count() == 1

        _do_update(tracker, "flight-002")
        assert tracker.count() == 2

        tracker.pop("flight-001")
        assert tracker.count() == 1

    def test_snapshot_returns_copy(self):
        tracker = DroneStateTracker()
        _do_update(tracker, "flight-001")
        snap = tracker.snapshot()

        assert "flight-001" in snap
        # Modifying the snapshot should not affect the tracker
        snap.pop("flight-001")
        assert tracker.get("flight-001") is not None


# ── Multiple independent sessions ────────────────────────────────────────────


class TestMultipleSessions:
    def test_sessions_tracked_independently(self):
        tracker = DroneStateTracker()
        _do_update(tracker, "flight-A", drone_id="drone-A", position=_make_position(alt=50.0), voltage=16.0)
        _do_update(tracker, "flight-B", drone_id="drone-B", position=_make_position(alt=100.0), voltage=15.0)

        # Update only flight-A with a higher altitude
        _do_update(tracker, "flight-A", drone_id="drone-A", position=_make_position(alt=80.0), voltage=15.5)

        state_a = tracker.get("flight-A")
        state_b = tracker.get("flight-B")

        assert state_a.max_altitude == 80.0
        assert state_a.message_count == 2
        assert state_a.voltage_out == 15.5

        assert state_b.max_altitude == 100.0
        assert state_b.message_count == 1
        assert state_b.voltage_out == 15.0


# ── _extract_voltage() ──────────────────────────────────────────────────────


class TestExtractVoltage:
    def test_from_battery_nested(self):
        payload = {"status": {"battery": {"voltage": 16.2}}}
        assert _extract_voltage(payload) == 16.2

    def test_from_status_level(self):
        payload = {"status": {"voltage": 15.9}}
        assert _extract_voltage(payload) == 15.9

    def test_from_top_level(self):
        payload = {"voltage": 14.8}
        assert _extract_voltage(payload) == 14.8

    def test_missing_returns_none(self):
        assert _extract_voltage({}) is None

    def test_non_numeric_returns_none(self):
        payload = {"status": {"battery": {"voltage": "bad"}}}
        assert _extract_voltage(payload) is None


# ── _safe_float() ────────────────────────────────────────────────────────────


class TestSafeFloat:
    def test_int(self):
        assert _safe_float(42) == 42.0

    def test_string_number(self):
        assert _safe_float("16.5") == 16.5

    def test_none(self):
        assert _safe_float(None) is None

    def test_invalid_string(self):
        assert _safe_float("abc") is None
