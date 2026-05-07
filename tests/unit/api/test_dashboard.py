"""Unit tests for the dashboard snapshot builder.

The HTML page itself is just a static string — there's nothing to unit
test there.  ``build_dashboard_snapshot`` is the load-bearing piece:
it walks the registry + tracker, classifies each session into one of
five status states, attaches the sweeper-stamped flags, and groups by
zone.  These tests pin that classification logic.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.api.dashboard import build_dashboard_snapshot
from app.monitoring.active_session_registry import (
    ActiveFlightSession,
    ActiveSessionRegistry,
)
from app.monitoring.state_tracker import (
    DroneState,
    DroneStateTracker,
    FlightSegment,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_session(
    flight_session_id: str = "flight-001",
    *,
    drone_id: str | None = "drone-01",
    pilot_id: str | None = "pilot-01",
    sade_zone_id: str | None = "zone-001",
    requested_exit_time: str | None = None,
    exit_requested_at: str | None = None,
    exit_deadline_breached_at: str | None = None,
    stranded_flagged_at: str | None = None,
) -> ActiveFlightSession:
    return ActiveFlightSession(
        flight_session_id=flight_session_id,
        drone_id=drone_id,
        pilot_id=pilot_id,
        sade_zone_id=sade_zone_id,
        requested_exit_time=requested_exit_time,
        exit_requested_at=exit_requested_at,
        exit_deadline_breached_at=exit_deadline_breached_at,
        stranded_flagged_at=stranded_flagged_at,
    )


def _make_registry(*sessions: ActiveFlightSession) -> ActiveSessionRegistry:
    reg = ActiveSessionRegistry()
    for s in sessions:
        reg.register(s)
    return reg


def _populate(
    tracker: DroneStateTracker,
    flight_session_id: str,
    *,
    last_seen: str | None = None,
    voltage: float | None = None,
    altitude: float | None = None,
    armed: bool | None = None,
) -> DroneState:
    """Drive one telemetry update through the tracker the same way the
    worker would.  Returns the resulting DroneState for further mutation
    in tests that need specific segment shapes."""
    payload: dict = {}
    status: dict = {}
    if voltage is not None:
        status["battery"] = {"voltage": voltage}
    if armed is not None:
        status["armed"] = armed
    position = None
    if altitude is not None:
        position = {"latitude": 39.77, "longitude": -86.16, "altitude": altitude}
        status.setdefault("location", {"altitude": altitude})
    if status:
        payload["status"] = status
    return tracker.update(
        flight_session_id,
        drone_id=f"drone-{flight_session_id}",
        session_source="aws",
        raw_message={},
        parsed_payload=payload,
        mission_status=None,
        mode=None,
        position=position,
        armed=armed,
        last_seen=last_seen,
    )


# ── Top-level shape ──────────────────────────────────────────────────────────


class TestSnapshotShape:
    def test_empty_registry(self):
        reg = ActiveSessionRegistry()
        tracker = DroneStateTracker()

        snap = build_dashboard_snapshot(reg, tracker)

        assert set(snap.keys()) == {"report_time_utc", "thresholds", "totals", "zones"}
        assert snap["zones"] == []
        assert snap["totals"] == {
            "active_sessions": 0,
            "sessions_past_deadline": 0,
            "sessions_stranded": 0,
        }

    def test_thresholds_block_present(self):
        snap = build_dashboard_snapshot(ActiveSessionRegistry(), DroneStateTracker())
        assert "stranded_silence_seconds" in snap["thresholds"]
        assert "force_close_threshold_seconds" in snap["thresholds"]
        # Sanity: production defaults are positive numbers, not None / 0.
        assert snap["thresholds"]["stranded_silence_seconds"] > 0
        assert snap["thresholds"]["force_close_threshold_seconds"] > 0

    def test_report_time_utc_is_iso_with_offset(self):
        snap = build_dashboard_snapshot(ActiveSessionRegistry(), DroneStateTracker())
        # Round-trip through fromisoformat to confirm it parses; check tz aware.
        dt = datetime.fromisoformat(snap["report_time_utc"])
        assert dt.tzinfo is not None


# ── Status classification ────────────────────────────────────────────────────


class TestStatusClassification:
    def test_waiting_no_telemetry(self):
        reg = _make_registry(_make_session("flight-1"))
        tracker = DroneStateTracker()

        snap = build_dashboard_snapshot(reg, tracker)
        s = snap["zones"][0]["sessions"][0]
        assert s["status"] == "WAITING"
        assert s["live"] is None

    def test_flying_open_segment(self):
        reg = _make_registry(_make_session("flight-1"))
        tracker = DroneStateTracker()
        _populate(tracker, "flight-1", last_seen="2026-03-09T18:05:00+00:00",
                  armed=True, voltage=16.4)

        snap = build_dashboard_snapshot(reg, tracker)
        s = snap["zones"][0]["sessions"][0]
        assert s["status"] == "FLYING"
        assert s["live"]["current_segment_open"] is True
        assert s["live"]["completed_segments"] == 0

    def test_landed_after_disarm(self):
        reg = _make_registry(_make_session("flight-1"))
        tracker = DroneStateTracker()
        _populate(tracker, "flight-1", last_seen="2026-03-09T18:00:00+00:00",
                  armed=True, voltage=16.4)
        _populate(tracker, "flight-1", last_seen="2026-03-09T18:30:00+00:00",
                  armed=False, voltage=14.9)

        snap = build_dashboard_snapshot(reg, tracker)
        s = snap["zones"][0]["sessions"][0]
        assert s["status"] == "LANDED"
        assert s["live"]["current_segment_open"] is False
        assert s["live"]["completed_segments"] == 1

    def test_landed_armed_seen_but_never_armed(self):
        """Drone has reported state but only armed=False so far —
        on the ground, hasn't taken off yet."""
        reg = _make_registry(_make_session("flight-1"))
        tracker = DroneStateTracker()
        _populate(tracker, "flight-1", last_seen="2026-03-09T18:00:00+00:00",
                  armed=False, voltage=16.5)

        snap = build_dashboard_snapshot(reg, tracker)
        s = snap["zones"][0]["sessions"][0]
        assert s["status"] == "LANDED"
        assert s["live"]["completed_segments"] == 0
        assert s["live"]["current_segment_open"] is False

    def test_active_legacy_firmware_no_armed_field(self):
        """Telemetry present but firmware doesn't emit status.armed."""
        reg = _make_registry(_make_session("flight-1"))
        tracker = DroneStateTracker()
        _populate(tracker, "flight-1", last_seen="2026-03-09T18:00:00+00:00",
                  voltage=16.0)  # no armed kwarg

        snap = build_dashboard_snapshot(reg, tracker)
        s = snap["zones"][0]["sessions"][0]
        assert s["status"] == "ACTIVE"

    def test_exit_requested_overrides_other_states(self):
        """Even if the drone is currently flying, an exit-request flips
        the badge to EXIT_REQUESTED so the operator sees the closeout
        is in progress."""
        reg = _make_registry(
            _make_session(
                "flight-1",
                exit_requested_at="2026-03-09T18:35:00+00:00",
            )
        )
        tracker = DroneStateTracker()
        _populate(tracker, "flight-1", last_seen="2026-03-09T18:34:00+00:00",
                  armed=True, voltage=14.0)

        snap = build_dashboard_snapshot(reg, tracker)
        assert snap["zones"][0]["sessions"][0]["status"] == "EXIT_REQUESTED"


# ── Flag attachment (independent of status) ──────────────────────────────────


class TestFlags:
    def test_no_flags(self):
        reg = _make_registry(_make_session("flight-1"))
        snap = build_dashboard_snapshot(reg, DroneStateTracker())
        assert snap["zones"][0]["sessions"][0]["flags"] == []

    def test_past_deadline_flag(self):
        reg = _make_registry(
            _make_session(
                "flight-1",
                exit_deadline_breached_at="2026-03-09T19:05:00+00:00",
            )
        )
        snap = build_dashboard_snapshot(reg, DroneStateTracker())
        assert snap["zones"][0]["sessions"][0]["flags"] == ["past_deadline"]

    def test_stranded_flag(self):
        reg = _make_registry(
            _make_session(
                "flight-1",
                stranded_flagged_at="2026-03-09T19:10:00+00:00",
            )
        )
        snap = build_dashboard_snapshot(reg, DroneStateTracker())
        assert snap["zones"][0]["sessions"][0]["flags"] == ["stranded"]

    def test_both_flags(self):
        reg = _make_registry(
            _make_session(
                "flight-1",
                exit_deadline_breached_at="2026-03-09T19:05:00+00:00",
                stranded_flagged_at="2026-03-09T19:10:00+00:00",
            )
        )
        snap = build_dashboard_snapshot(reg, DroneStateTracker())
        assert snap["zones"][0]["sessions"][0]["flags"] == ["past_deadline", "stranded"]

    def test_flags_independent_of_status(self):
        """A FLYING drone can also be flagged stranded — the flag came
        from a previous silence window even though telemetry has now
        resumed.  The dashboard surfaces both."""
        reg = _make_registry(
            _make_session(
                "flight-1",
                stranded_flagged_at="2026-03-09T19:10:00+00:00",
            )
        )
        tracker = DroneStateTracker()
        _populate(tracker, "flight-1", last_seen="2026-03-09T19:30:00+00:00",
                  armed=True, voltage=15.0)
        snap = build_dashboard_snapshot(reg, tracker)
        s = snap["zones"][0]["sessions"][0]
        assert s["status"] == "FLYING"
        assert "stranded" in s["flags"]


# ── Zone grouping ────────────────────────────────────────────────────────────


class TestZoneGrouping:
    def test_sessions_grouped_by_zone(self):
        reg = _make_registry(
            _make_session("flight-A", drone_id="d-A", sade_zone_id="zone-001"),
            _make_session("flight-B", drone_id="d-B", sade_zone_id="zone-001"),
            _make_session("flight-C", drone_id="d-C", sade_zone_id="zone-002"),
        )
        snap = build_dashboard_snapshot(reg, DroneStateTracker())

        zone_ids = [z["sade_zone_id"] for z in snap["zones"]]
        assert zone_ids == ["zone-001", "zone-002"]
        assert len(snap["zones"][0]["sessions"]) == 2
        assert len(snap["zones"][1]["sessions"]) == 1

    def test_session_with_null_zone_bucketed_under_unspecified(self):
        reg = _make_registry(
            _make_session("flight-A", drone_id="d-A", sade_zone_id=None),
            _make_session("flight-B", drone_id="d-B", sade_zone_id="zone-001"),
        )
        snap = build_dashboard_snapshot(reg, DroneStateTracker())

        zone_ids = sorted(z["sade_zone_id"] for z in snap["zones"])
        assert "(unspecified)" in zone_ids
        assert "zone-001" in zone_ids

    def test_zones_are_sorted_alphabetically(self):
        """Stable ordering across polls keeps the page from reshuffling."""
        reg = _make_registry(
            _make_session("flight-A", drone_id="d-A", sade_zone_id="zone-z"),
            _make_session("flight-B", drone_id="d-B", sade_zone_id="zone-a"),
            _make_session("flight-C", drone_id="d-C", sade_zone_id="zone-m"),
        )
        snap = build_dashboard_snapshot(reg, DroneStateTracker())
        assert [z["sade_zone_id"] for z in snap["zones"]] == ["zone-a", "zone-m", "zone-z"]


# ── Live telemetry block ─────────────────────────────────────────────────────


class TestLiveTelemetry:
    def test_seconds_since_last_seen_is_present(self):
        reg = _make_registry(_make_session("flight-1"))
        tracker = DroneStateTracker()
        last = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        _populate(tracker, "flight-1", last_seen=last, armed=True, voltage=16.0)

        snap = build_dashboard_snapshot(reg, tracker)
        secs = snap["zones"][0]["sessions"][0]["live"]["seconds_since_last_seen"]
        # Allow some test-runtime jitter.
        assert 25 <= secs <= 60

    def test_altitude_voltage_distance_forwarded(self):
        reg = _make_registry(_make_session("flight-1"))
        tracker = DroneStateTracker()
        _populate(tracker, "flight-1", last_seen="2026-03-09T18:00:00+00:00",
                  armed=True, voltage=16.4, altitude=110.0)

        live = build_dashboard_snapshot(reg, tracker)["zones"][0]["sessions"][0]["live"]
        assert live["altitude_m"] == 110.0
        assert live["voltage_v"] == 16.4
        assert live["distance_flown_m"] == 0.0  # one fix, no haversine yet

    def test_completed_segments_count(self):
        reg = _make_registry(_make_session("flight-1"))
        tracker = DroneStateTracker()
        # Manually construct a state with two closed segments and one open.
        state = _populate(tracker, "flight-1", last_seen="2026-03-09T18:00:00+00:00",
                          armed=True, voltage=16.0)
        state.segments = [
            FlightSegment("2026-03-09T18:00:00+00:00", "2026-03-09T18:30:00+00:00", 16.0, 14.0),
            FlightSegment("2026-03-09T19:00:00+00:00", "2026-03-09T19:30:00+00:00", 16.0, 14.0),
            FlightSegment("2026-03-09T20:00:00+00:00", None, 16.0, None),
        ]

        live = build_dashboard_snapshot(reg, tracker)["zones"][0]["sessions"][0]["live"]
        assert live["completed_segments"] == 2
        assert live["current_segment_open"] is True

    def test_unparseable_last_seen_yields_null_seconds_since_last_seen(self):
        """A malformed timestamp shouldn't crash the snapshot — surface
        as null and let the page render '—'."""
        reg = _make_registry(_make_session("flight-1"))
        tracker = DroneStateTracker()
        state = _populate(tracker, "flight-1", last_seen="2026-03-09T18:00:00+00:00",
                          armed=True, voltage=16.0)
        state.last_seen = "not-a-date"

        live = build_dashboard_snapshot(reg, tracker)["zones"][0]["sessions"][0]["live"]
        assert live["seconds_since_last_seen"] is None


# ── Totals (echo of registry counts) ─────────────────────────────────────────


class TestTotals:
    def test_totals_match_registry_counts(self):
        reg = _make_registry(
            _make_session("flight-A", drone_id="d-A"),
            _make_session(
                "flight-B", drone_id="d-B",
                exit_deadline_breached_at="2026-03-09T19:05:00+00:00",
            ),
            _make_session(
                "flight-C", drone_id="d-C",
                stranded_flagged_at="2026-03-09T19:10:00+00:00",
            ),
        )
        snap = build_dashboard_snapshot(reg, DroneStateTracker())
        assert snap["totals"] == {
            "active_sessions": 3,
            "sessions_past_deadline": 1,
            "sessions_stranded": 1,
        }
