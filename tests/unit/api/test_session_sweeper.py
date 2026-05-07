"""Unit tests for the deadline-breach session sweeper.

The sweeper's per-iteration logic lives in ``_scan_for_deadline_breaches``,
which is pulled out of the async loop precisely so it can be exercised
synchronously here without mocking asyncio.sleep / event loops.

Each test constructs a fresh ``ActiveSessionRegistry``, populates it with
sessions in specific states, calls ``_scan_for_deadline_breaches(reg)``,
and asserts on the returned newly-flagged count plus the post-call state
of each session's ``exit_deadline_breached_at`` field.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.api.server import (
    STRANDED_SILENCE_THRESHOLD_SECONDS,
    _scan_for_deadline_breaches,
    _scan_for_stranded_sessions,
)
from app.monitoring.active_session_registry import (
    ActiveFlightSession,
    ActiveSessionRegistry,
)
from app.monitoring.state_tracker import DroneStateTracker


def _iso(dt: datetime) -> str:
    """Helper: produce the ISO 8601 strings SADE actually sends."""
    return dt.isoformat()


def _make_session(
    flight_session_id: str,
    *,
    requested_exit_time: str | None = None,
    exit_requested_at: str | None = None,
    exit_deadline_breached_at: str | None = None,
    stranded_flagged_at: str | None = None,
) -> ActiveFlightSession:
    return ActiveFlightSession(
        flight_session_id=flight_session_id,
        drone_id=f"drone-{flight_session_id}",
        requested_exit_time=requested_exit_time,
        exit_requested_at=exit_requested_at,
        exit_deadline_breached_at=exit_deadline_breached_at,
        stranded_flagged_at=stranded_flagged_at,
    )


def _populate_tracker(
    tracker: DroneStateTracker,
    flight_session_id: str,
    last_seen: str,
    *,
    drone_id: str | None = None,
) -> None:
    """Inject one telemetry update so the session has a DroneState with the
    given last_seen timestamp.  Mirrors what the worker does in production."""
    tracker.update(
        flight_session_id,
        drone_id=drone_id or f"drone-{flight_session_id}",
        session_source="aws",
        raw_message={},
        parsed_payload={},
        mission_status=None,
        mode=None,
        position=None,
        last_seen=last_seen,
    )


class TestNoFlagging:
    def test_empty_registry(self):
        reg = ActiveSessionRegistry()

        assert _scan_for_deadline_breaches(reg) == 0

    def test_session_without_requested_exit_time(self):
        """SADE didn't supply a deadline — sweeper has nothing to compare against."""
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1", requested_exit_time=None))

        assert _scan_for_deadline_breaches(reg) == 0
        assert reg.get_by_flight_session_id("flight-1").exit_deadline_breached_at is None

    def test_session_within_authorized_window(self):
        """Deadline is in the future — no breach yet."""
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1", requested_exit_time=_iso(future)))

        assert _scan_for_deadline_breaches(reg) == 0
        assert reg.get_by_flight_session_id("flight-1").exit_deadline_breached_at is None

    def test_session_exactly_at_deadline(self):
        """``now <= deadline`` is not a breach (strict inequality)."""
        deadline = datetime.now(timezone.utc) + timedelta(seconds=10)
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1", requested_exit_time=_iso(deadline)))

        assert _scan_for_deadline_breaches(reg) == 0


class TestFlagging:
    def test_session_past_deadline_no_exit_request(self):
        """The canonical case: deadline elapsed and SADE never closed."""
        past = datetime.now(timezone.utc) - timedelta(seconds=5)
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1", requested_exit_time=_iso(past)))

        assert _scan_for_deadline_breaches(reg) == 1

        session = reg.get_by_flight_session_id("flight-1")
        assert session.exit_deadline_breached_at is not None
        # Stamped value should round-trip as a UTC ISO timestamp.
        stamped = datetime.fromisoformat(session.exit_deadline_breached_at)
        assert stamped.tzinfo is not None

    def test_multiple_sessions_some_past_some_not(self):
        past = datetime.now(timezone.utc) - timedelta(seconds=5)
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        reg = ActiveSessionRegistry()
        reg.register(_make_session("breached-1", requested_exit_time=_iso(past)))
        reg.register(_make_session("breached-2", requested_exit_time=_iso(past)))
        reg.register(_make_session("ok-1", requested_exit_time=_iso(future)))
        reg.register(_make_session("nodeadline", requested_exit_time=None))

        assert _scan_for_deadline_breaches(reg) == 2
        assert reg.count_past_deadline() == 2

        assert reg.get_by_flight_session_id("breached-1").exit_deadline_breached_at is not None
        assert reg.get_by_flight_session_id("breached-2").exit_deadline_breached_at is not None
        assert reg.get_by_flight_session_id("ok-1").exit_deadline_breached_at is None
        assert reg.get_by_flight_session_id("nodeadline").exit_deadline_breached_at is None


class TestSkippedConditions:
    def test_session_with_exit_request_already_set(self):
        """SADE already closed it — sweeper shouldn't second-guess that."""
        past = datetime.now(timezone.utc) - timedelta(seconds=5)
        reg = ActiveSessionRegistry()
        reg.register(
            _make_session(
                "flight-1",
                requested_exit_time=_iso(past),
                exit_requested_at="2026-03-09T19:00:00+00:00",
            )
        )

        assert _scan_for_deadline_breaches(reg) == 0
        assert reg.get_by_flight_session_id("flight-1").exit_deadline_breached_at is None

    def test_already_flagged_session_not_reflagged(self):
        """One-shot edge detector: stamped timestamp must not be overwritten."""
        past = datetime.now(timezone.utc) - timedelta(seconds=5)
        original_flag = "2026-03-09T19:05:00+00:00"
        reg = ActiveSessionRegistry()
        reg.register(
            _make_session(
                "flight-1",
                requested_exit_time=_iso(past),
                exit_deadline_breached_at=original_flag,
            )
        )

        # Run the sweeper many times — the original timestamp must persist.
        for _ in range(5):
            assert _scan_for_deadline_breaches(reg) == 0

        assert reg.get_by_flight_session_id("flight-1").exit_deadline_breached_at == original_flag

    def test_unparseable_deadline_does_not_crash(self):
        """A malformed timestamp should be skipped, not crash the sweeper."""
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1", requested_exit_time="not-a-date"))
        # Also register a real breach in the same registry — it should still
        # be flagged even when an earlier session has a bad timestamp.
        past = datetime.now(timezone.utc) - timedelta(seconds=5)
        reg.register(_make_session("flight-2", requested_exit_time=_iso(past)))

        assert _scan_for_deadline_breaches(reg) == 1
        assert reg.get_by_flight_session_id("flight-1").exit_deadline_breached_at is None
        assert reg.get_by_flight_session_id("flight-2").exit_deadline_breached_at is not None


class TestNaiveTimestamps:
    """Defence: SADE sends UTC offset/Z but a misconfigured sender might omit it."""

    def test_naive_past_deadline_treated_as_utc_breach(self):
        """A naive datetime is treated as UTC rather than crashing on compare."""
        # Build a naive ISO string by stripping the timezone.
        past_aware = datetime.now(timezone.utc) - timedelta(seconds=5)
        past_naive_iso = past_aware.replace(tzinfo=None).isoformat()
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1", requested_exit_time=past_naive_iso))

        assert _scan_for_deadline_breaches(reg) == 1
        assert reg.get_by_flight_session_id("flight-1").exit_deadline_breached_at is not None

    def test_naive_future_deadline_treated_as_utc_no_breach(self):
        future_aware = datetime.now(timezone.utc) + timedelta(hours=1)
        future_naive_iso = future_aware.replace(tzinfo=None).isoformat()
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1", requested_exit_time=future_naive_iso))

        assert _scan_for_deadline_breaches(reg) == 0


class TestCounterIntegration:
    """Verify the count_past_deadline() counter reflects sweeper output."""

    def test_count_increments_on_flag(self):
        past = datetime.now(timezone.utc) - timedelta(seconds=5)
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1", requested_exit_time=_iso(past)))
        assert reg.count_past_deadline() == 0

        _scan_for_deadline_breaches(reg)
        assert reg.count_past_deadline() == 1

    def test_count_unchanged_on_repeat_sweep(self):
        past = datetime.now(timezone.utc) - timedelta(seconds=5)
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1", requested_exit_time=_iso(past)))

        _scan_for_deadline_breaches(reg)
        first_count = reg.count_past_deadline()
        _scan_for_deadline_breaches(reg)
        second_count = reg.count_past_deadline()

        assert first_count == second_count == 1


# ── _scan_for_stranded_sessions ──────────────────────────────────────────────
#
# Stranded means: drone was transmitting (DroneState exists), it has now been
# silent for longer than STRANDED_SILENCE_THRESHOLD_SECONDS, and SADE has not
# sent an exit-request.


def _silent_for(seconds: float) -> str:
    """Return an ISO timestamp that's ``seconds`` in the past."""
    return _iso(datetime.now(timezone.utc) - timedelta(seconds=seconds))


class TestStrandedNoFlagging:
    def test_empty_registry(self):
        reg = ActiveSessionRegistry()
        tracker = DroneStateTracker()

        assert _scan_for_stranded_sessions(reg, tracker) == 0

    def test_session_with_no_telemetry(self):
        """No DroneState exists yet — explicitly NOT a stranded case
        (could be a drone that just hasn't taken off).  Deadline-breach
        detector handles 'never showed up' via requested_exit_time."""
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1"))
        tracker = DroneStateTracker()  # empty

        assert _scan_for_stranded_sessions(reg, tracker) == 0
        assert reg.get_by_flight_session_id("flight-1").stranded_flagged_at is None

    def test_session_with_recent_telemetry(self):
        """Telemetry just arrived — not silent yet."""
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1"))
        tracker = DroneStateTracker()
        _populate_tracker(tracker, "flight-1", _silent_for(10.0))

        assert _scan_for_stranded_sessions(reg, tracker) == 0

    def test_session_silent_just_under_threshold(self):
        """Silence below threshold doesn't flag (strict comparison)."""
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1"))
        tracker = DroneStateTracker()
        _populate_tracker(
            tracker, "flight-1",
            _silent_for(STRANDED_SILENCE_THRESHOLD_SECONDS - 1.0),
        )

        assert _scan_for_stranded_sessions(reg, tracker) == 0


class TestStrandedFlagging:
    def test_session_silent_past_threshold(self):
        """Canonical case: drone was transmitting, hasn't said anything in
        longer than the silence threshold, no exit-request received."""
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1"))
        tracker = DroneStateTracker()
        _populate_tracker(
            tracker, "flight-1",
            _silent_for(STRANDED_SILENCE_THRESHOLD_SECONDS + 30.0),
        )

        assert _scan_for_stranded_sessions(reg, tracker) == 1

        session = reg.get_by_flight_session_id("flight-1")
        assert session.stranded_flagged_at is not None
        # Stamped value should be a valid UTC ISO timestamp.
        stamped = datetime.fromisoformat(session.stranded_flagged_at)
        assert stamped.tzinfo is not None

    def test_multiple_sessions_some_silent_some_active(self):
        reg = ActiveSessionRegistry()
        reg.register(_make_session("silent-1"))
        reg.register(_make_session("silent-2"))
        reg.register(_make_session("active-1"))
        reg.register(_make_session("nostate"))  # no DroneState at all
        tracker = DroneStateTracker()
        _populate_tracker(tracker, "silent-1", _silent_for(STRANDED_SILENCE_THRESHOLD_SECONDS + 60))
        _populate_tracker(tracker, "silent-2", _silent_for(STRANDED_SILENCE_THRESHOLD_SECONDS + 60))
        _populate_tracker(tracker, "active-1", _silent_for(5.0))

        assert _scan_for_stranded_sessions(reg, tracker) == 2
        assert reg.count_stranded() == 2

        assert reg.get_by_flight_session_id("silent-1").stranded_flagged_at is not None
        assert reg.get_by_flight_session_id("silent-2").stranded_flagged_at is not None
        assert reg.get_by_flight_session_id("active-1").stranded_flagged_at is None
        assert reg.get_by_flight_session_id("nostate").stranded_flagged_at is None


class TestStrandedSkippedConditions:
    def test_session_with_exit_request_already_set(self):
        """SADE has already closed it — sweeper shouldn't second-guess.
        Even if telemetry has gone silent past the threshold, the grace
        task owns finalization in this case."""
        reg = ActiveSessionRegistry()
        reg.register(
            _make_session(
                "flight-1",
                exit_requested_at="2026-03-09T19:00:00+00:00",
            )
        )
        tracker = DroneStateTracker()
        _populate_tracker(
            tracker, "flight-1",
            _silent_for(STRANDED_SILENCE_THRESHOLD_SECONDS + 60),
        )

        assert _scan_for_stranded_sessions(reg, tracker) == 0
        assert reg.get_by_flight_session_id("flight-1").stranded_flagged_at is None

    def test_already_flagged_session_not_reflagged(self):
        """One-shot edge detector — original timestamp must persist."""
        original_flag = "2026-03-09T19:10:00+00:00"
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1", stranded_flagged_at=original_flag))
        tracker = DroneStateTracker()
        _populate_tracker(
            tracker, "flight-1",
            _silent_for(STRANDED_SILENCE_THRESHOLD_SECONDS + 60),
        )

        for _ in range(5):
            assert _scan_for_stranded_sessions(reg, tracker) == 0

        assert reg.get_by_flight_session_id("flight-1").stranded_flagged_at == original_flag

    def test_unparseable_last_seen_does_not_crash(self):
        """A malformed timestamp on DroneState should be skipped.  Must not
        prevent OTHER stranded sessions from being flagged on the same pass."""
        reg = ActiveSessionRegistry()
        reg.register(_make_session("bad-timestamp"))
        reg.register(_make_session("good-stranded"))
        tracker = DroneStateTracker()
        _populate_tracker(tracker, "bad-timestamp", "not-a-date")
        _populate_tracker(
            tracker, "good-stranded",
            _silent_for(STRANDED_SILENCE_THRESHOLD_SECONDS + 60),
        )

        assert _scan_for_stranded_sessions(reg, tracker) == 1
        assert reg.get_by_flight_session_id("bad-timestamp").stranded_flagged_at is None
        assert reg.get_by_flight_session_id("good-stranded").stranded_flagged_at is not None


class TestStrandedNaiveTimestamps:
    def test_naive_silent_treated_as_utc_flagged(self):
        """A naive last_seen string is treated as UTC, not crashed on."""
        naive_iso = (
            datetime.now(timezone.utc)
            - timedelta(seconds=STRANDED_SILENCE_THRESHOLD_SECONDS + 60)
        ).replace(tzinfo=None).isoformat()
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1"))
        tracker = DroneStateTracker()
        _populate_tracker(tracker, "flight-1", naive_iso)

        assert _scan_for_stranded_sessions(reg, tracker) == 1


class TestBothFlagsCanFireOnSameSession:
    """Deadline breach and stranded are independent signals.  A session
    that's both past its deadline AND has gone silent past the threshold
    must end up with BOTH flags set."""

    def test_both_flags_set_independently(self):
        past = datetime.now(timezone.utc) - timedelta(seconds=10)
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1", requested_exit_time=_iso(past)))
        tracker = DroneStateTracker()
        _populate_tracker(
            tracker, "flight-1",
            _silent_for(STRANDED_SILENCE_THRESHOLD_SECONDS + 60),
        )

        # First scan: deadline breach.
        assert _scan_for_deadline_breaches(reg) == 1
        # Second scan: also stranded.
        assert _scan_for_stranded_sessions(reg, tracker) == 1

        session = reg.get_by_flight_session_id("flight-1")
        assert session.exit_deadline_breached_at is not None
        assert session.stranded_flagged_at is not None
        assert reg.count_past_deadline() == 1
        assert reg.count_stranded() == 1


class TestStrandedCounterIntegration:
    def test_count_increments_on_flag(self):
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1"))
        tracker = DroneStateTracker()
        _populate_tracker(
            tracker, "flight-1",
            _silent_for(STRANDED_SILENCE_THRESHOLD_SECONDS + 60),
        )
        assert reg.count_stranded() == 0

        _scan_for_stranded_sessions(reg, tracker)
        assert reg.count_stranded() == 1

    def test_count_unchanged_on_repeat_sweep(self):
        reg = ActiveSessionRegistry()
        reg.register(_make_session("flight-1"))
        tracker = DroneStateTracker()
        _populate_tracker(
            tracker, "flight-1",
            _silent_for(STRANDED_SILENCE_THRESHOLD_SECONDS + 60),
        )

        _scan_for_stranded_sessions(reg, tracker)
        first_count = reg.count_stranded()
        _scan_for_stranded_sessions(reg, tracker)
        second_count = reg.count_stranded()

        assert first_count == second_count == 1
