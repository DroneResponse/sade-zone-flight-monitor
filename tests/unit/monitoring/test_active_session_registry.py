"""Unit tests for app.monitoring.active_session_registry."""

from __future__ import annotations

import pytest

from app.monitoring.active_session_registry import (
    ActiveFlightSession,
    ActiveSessionRegistry,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_session(
    flight_session_id: str = "flight-001",
    *,
    drone_id: str | None = "drone-01",
    session_source: str = "aws",
    **kwargs,
) -> ActiveFlightSession:
    return ActiveFlightSession(
        flight_session_id=flight_session_id,
        drone_id=drone_id,
        session_source=session_source,
        **kwargs,
    )


# ── ActiveFlightSession dataclass ────────────────────────────────────────────


class TestActiveFlightSession:
    def test_minimal_creation(self):
        session = ActiveFlightSession(flight_session_id="flight-001")

        assert session.flight_session_id == "flight-001"
        assert session.evaluation_series_id == ""
        assert session.drone_id is None
        assert session.pilot_id is None
        assert session.session_source == "aws"
        assert session.test_overrides is None
        assert session.requested_operation is None
        assert session.submitted_at is None
        assert isinstance(session.registered_at, str)
        assert len(session.registered_at) > 0

    def test_full_creation(self):
        session = ActiveFlightSession(
            flight_session_id="flight-001",
            evaluation_series_id="eval-001",
            drone_id="drone-01",
            pilot_id="pilot-01",
            organization_id="org-01",
            sade_zone_id="zone-01",
            decision="REGISTERED",
            requested_entry_time="2026-01-01T00:00:00Z",
            requested_exit_time="2026-01-01T01:00:00Z",
            session_source="aws",
            requested_operation={"operation_type": "INSPECTION"},
            test_overrides={"actual_start_time": "2026-01-01T00:05:00Z"},
            submitted_at="2026-01-01T00:00:00Z",
            source_payload={"raw": True},
        )

        assert session.drone_id == "drone-01"
        assert session.pilot_id == "pilot-01"
        assert session.organization_id == "org-01"
        assert session.sade_zone_id == "zone-01"
        assert session.decision == "REGISTERED"
        assert session.requested_operation == {"operation_type": "INSPECTION"}
        assert session.test_overrides is not None
        assert session.source_payload == {"raw": True}


# ── register(): new sessions ─────────────────────────────────────────────────


class TestRegisterNew:
    def test_register_new_session(self):
        reg = ActiveSessionRegistry()
        session = _make_session("flight-001", drone_id="drone-01")
        result = reg.register(session)

        assert result is session
        assert reg.get_by_flight_session_id("flight-001") is session

    def test_register_session_without_drone_id(self):
        reg = ActiveSessionRegistry()
        session = _make_session("flight-001", drone_id=None)
        result = reg.register(session)

        assert result is session
        assert reg.get_by_flight_session_id("flight-001") is session
        # No drone index entry, so drone lookup returns None
        assert reg.get_by_drone_id("drone-01") is None

    def test_register_multiple_different_drones(self):
        reg = ActiveSessionRegistry()
        session_a = _make_session("flight-A", drone_id="drone-A")
        session_b = _make_session("flight-B", drone_id="drone-B")

        reg.register(session_a)
        reg.register(session_b)

        assert reg.get_by_drone_id("drone-A") is session_a
        assert reg.get_by_drone_id("drone-B") is session_b
        assert reg.count() == 2


# ── register(): re-registration (same flight_session_id) ────────────────────


class TestReregister:
    def test_reregister_same_flight_session_id_updates(self):
        reg = ActiveSessionRegistry()
        original = _make_session("flight-001", drone_id="drone-01", pilot_id="pilot-old")
        reg.register(original)

        updated = _make_session("flight-001", drone_id="drone-01", pilot_id="pilot-new")
        reg.register(updated)

        stored = reg.get_by_flight_session_id("flight-001")
        assert stored.pilot_id == "pilot-new"

    def test_reregister_updates_drone_index(self):
        reg = ActiveSessionRegistry()
        original = _make_session("flight-001", drone_id=None)
        reg.register(original)

        # Re-register same flight_session_id, now with a drone_id
        updated = _make_session("flight-001", drone_id="drone-01")
        reg.register(updated)

        assert reg.get_by_drone_id("drone-01") is updated


# ── register(): duplicate drone enforcement ──────────────────────────────────


class TestDuplicateDrone:
    def test_duplicate_drone_different_session_raises(self):
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-A", drone_id="drone-01"))

        with pytest.raises(ValueError, match="drone-01.*already has active flight session"):
            reg.register(_make_session("flight-B", drone_id="drone-01"))

    def test_duplicate_drone_same_session_allowed(self):
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-A", drone_id="drone-01"))
        # Re-registering the same flight_session_id should not raise
        reg.register(_make_session("flight-A", drone_id="drone-01"))

        assert reg.count() == 1


# ── get_by_flight_session_id() ───────────────────────────────────────────────


class TestGetByFlightSessionId:
    def test_found(self):
        reg = ActiveSessionRegistry()
        session = _make_session("flight-001")
        reg.register(session)

        assert reg.get_by_flight_session_id("flight-001") is session

    def test_not_found(self):
        reg = ActiveSessionRegistry()
        assert reg.get_by_flight_session_id("nonexistent") is None


# ── get_by_drone_id() ────────────────────────────────────────────────────────


class TestGetByDroneId:
    def test_found(self):
        reg = ActiveSessionRegistry()
        session = _make_session("flight-001", drone_id="drone-01")
        reg.register(session)

        assert reg.get_by_drone_id("drone-01") is session

    def test_not_found(self):
        reg = ActiveSessionRegistry()
        assert reg.get_by_drone_id("nonexistent") is None

    def test_no_drone_id_on_session(self):
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-001", drone_id=None))

        assert reg.get_by_drone_id("drone-01") is None


# ── complete() ───────────────────────────────────────────────────────────────


class TestComplete:
    def test_removes_session(self):
        reg = ActiveSessionRegistry()
        session = _make_session("flight-001", drone_id="drone-01")
        reg.register(session)

        result = reg.complete("flight-001")

        assert result is session
        assert reg.get_by_flight_session_id("flight-001") is None
        assert reg.get_by_drone_id("drone-01") is None

    def test_unknown_returns_none(self):
        reg = ActiveSessionRegistry()
        assert reg.complete("nonexistent") is None

    def test_frees_drone_for_new_session(self):
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-A", drone_id="drone-01"))
        reg.complete("flight-A")

        # drone-01 is now free — registering a new session should succeed
        new_session = _make_session("flight-B", drone_id="drone-01")
        reg.register(new_session)

        assert reg.get_by_drone_id("drone-01") is new_session

    def test_session_without_drone_id(self):
        reg = ActiveSessionRegistry()
        session = _make_session("flight-001", drone_id=None)
        reg.register(session)

        result = reg.complete("flight-001")

        assert result is session
        assert reg.count() == 0


# ── ensure_local_session() ───────────────────────────────────────────────────


class TestEnsureLocalSession:
    def test_creates_synthetic_session(self):
        reg = ActiveSessionRegistry()
        session = reg.ensure_local_session("drone-01")

        assert session.drone_id == "drone-01"
        assert session.session_source == "local"
        assert session.decision == "LOCAL_TEST_SESSION"
        assert session.flight_session_id.startswith("local-flight-")

    def test_reuses_existing_session(self):
        reg = ActiveSessionRegistry()
        first = reg.ensure_local_session("drone-01")
        second = reg.ensure_local_session("drone-01")

        assert first.flight_session_id == second.flight_session_id
        assert reg.count() == 1

    def test_local_session_is_registered(self):
        reg = ActiveSessionRegistry()
        session = reg.ensure_local_session("drone-01")

        assert reg.get_by_flight_session_id(session.flight_session_id) is session
        assert reg.get_by_drone_id("drone-01") is session


# ── snapshot() and count() ───────────────────────────────────────────────────


class TestSnapshotAndCount:
    def test_count_empty(self):
        reg = ActiveSessionRegistry()
        assert reg.count() == 0

    def test_count_after_register_and_complete(self):
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-A", drone_id="drone-A"))
        reg.register(_make_session("flight-B", drone_id="drone-B"))
        assert reg.count() == 2

        reg.complete("flight-A")
        assert reg.count() == 1

    def test_snapshot_returns_copy(self):
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-001"))
        snap = reg.snapshot()

        assert "flight-001" in snap
        snap.pop("flight-001")
        # Modifying snapshot should not affect the registry
        assert reg.get_by_flight_session_id("flight-001") is not None


# ── Deadline-breach tracking ─────────────────────────────────────────────────


class TestDeadlineFields:
    def test_default_field_values(self):
        """The new sweeper-related fields default to None."""
        session = ActiveFlightSession(flight_session_id="flight-001")

        assert session.exit_requested_at is None
        assert session.exit_deadline_breached_at is None
        assert session.stranded_flagged_at is None

    def test_fields_can_be_set(self):
        session = ActiveFlightSession(
            flight_session_id="flight-001",
            exit_requested_at="2026-03-09T19:00:00+00:00",
            exit_deadline_breached_at="2026-03-09T19:05:00+00:00",
            stranded_flagged_at="2026-03-09T19:10:00+00:00",
        )

        assert session.exit_requested_at == "2026-03-09T19:00:00+00:00"
        assert session.exit_deadline_breached_at == "2026-03-09T19:05:00+00:00"
        assert session.stranded_flagged_at == "2026-03-09T19:10:00+00:00"


class TestCountPastDeadline:
    def test_empty_registry(self):
        reg = ActiveSessionRegistry()
        assert reg.count_past_deadline() == 0

    def test_no_sessions_flagged(self):
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-A", drone_id="drone-A"))
        reg.register(_make_session("flight-B", drone_id="drone-B"))

        assert reg.count_past_deadline() == 0

    def test_one_session_flagged(self):
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-A", drone_id="drone-A"))
        flagged = _make_session(
            "flight-B",
            drone_id="drone-B",
            exit_deadline_breached_at="2026-03-09T19:05:00+00:00",
        )
        reg.register(flagged)

        assert reg.count_past_deadline() == 1

    def test_mixed_set_and_unset(self):
        reg = ActiveSessionRegistry()
        for i in range(3):
            reg.register(
                _make_session(
                    f"flight-flagged-{i}",
                    drone_id=f"drone-flagged-{i}",
                    exit_deadline_breached_at="2026-03-09T19:05:00+00:00",
                )
            )
        for i in range(2):
            reg.register(
                _make_session(
                    f"flight-clean-{i}",
                    drone_id=f"drone-clean-{i}",
                )
            )

        assert reg.count_past_deadline() == 3
        assert reg.count() == 5

    def test_count_drops_when_session_completes(self):
        """Completing a flagged session removes it from the past-deadline count."""
        reg = ActiveSessionRegistry()
        reg.register(
            _make_session(
                "flight-A",
                drone_id="drone-A",
                exit_deadline_breached_at="2026-03-09T19:05:00+00:00",
            )
        )
        assert reg.count_past_deadline() == 1

        reg.complete("flight-A")
        assert reg.count_past_deadline() == 0


class TestCountStranded:
    def test_empty_registry(self):
        reg = ActiveSessionRegistry()
        assert reg.count_stranded() == 0

    def test_no_sessions_flagged(self):
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-A", drone_id="drone-A"))
        reg.register(_make_session("flight-B", drone_id="drone-B"))

        assert reg.count_stranded() == 0

    def test_one_session_flagged(self):
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-A", drone_id="drone-A"))
        flagged = _make_session(
            "flight-B",
            drone_id="drone-B",
            stranded_flagged_at="2026-03-09T19:10:00+00:00",
        )
        reg.register(flagged)

        assert reg.count_stranded() == 1

    def test_mixed_set_and_unset(self):
        reg = ActiveSessionRegistry()
        for i in range(3):
            reg.register(
                _make_session(
                    f"flight-stranded-{i}",
                    drone_id=f"drone-stranded-{i}",
                    stranded_flagged_at="2026-03-09T19:10:00+00:00",
                )
            )
        for i in range(2):
            reg.register(
                _make_session(
                    f"flight-clean-{i}",
                    drone_id=f"drone-clean-{i}",
                )
            )

        assert reg.count_stranded() == 3
        assert reg.count() == 5

    def test_count_drops_when_session_completes(self):
        reg = ActiveSessionRegistry()
        reg.register(
            _make_session(
                "flight-A",
                drone_id="drone-A",
                stranded_flagged_at="2026-03-09T19:10:00+00:00",
            )
        )
        assert reg.count_stranded() == 1

        reg.complete("flight-A")
        assert reg.count_stranded() == 0

    def test_counters_are_independent(self):
        """A session can carry both flags simultaneously and each counter
        reports them correctly."""
        reg = ActiveSessionRegistry()
        reg.register(
            _make_session(
                "flight-both",
                drone_id="drone-both",
                exit_deadline_breached_at="2026-03-09T19:05:00+00:00",
                stranded_flagged_at="2026-03-09T19:10:00+00:00",
            )
        )

        assert reg.count_past_deadline() == 1
        assert reg.count_stranded() == 1
        assert reg.count() == 1
