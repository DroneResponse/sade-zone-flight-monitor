"""Unit tests for the SADE Central drone-snapshot endpoint.

Covers the flat-list shape, the per-status totals aggregation, the
per-flag totals aggregation, and the per-drone entry builder
(currently_armed / live block / position handling).  Mirrors the
helper conventions from test_dashboard.py so the two suites read the
same way.
"""

from __future__ import annotations

from app.api.sade_central import build_drone_snapshot
from app.monitoring.active_session_registry import (
    ActiveFlightSession,
    ActiveSessionRegistry,
)
from app.monitoring.state_tracker import DroneStateTracker


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_session(
    flight_session_id: str = "flight-001",
    *,
    drone_id: str | None = "drone-01",
    pilot_id: str | None = "pilot-01",
    organization_id: str | None = "org-xyz",
    sade_zone_id: str | None = "zone-001",
    requested_entry_time: str | None = "2026-05-12T17:00:00+00:00",
    requested_exit_time: str | None = "2026-05-12T18:00:00+00:00",
    exit_requested_at: str | None = None,
    exit_deadline_breached_at: str | None = None,
    stranded_flagged_at: str | None = None,
) -> ActiveFlightSession:
    return ActiveFlightSession(
        flight_session_id=flight_session_id,
        drone_id=drone_id,
        pilot_id=pilot_id,
        organization_id=organization_id,
        sade_zone_id=sade_zone_id,
        requested_entry_time=requested_entry_time,
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
) -> None:
    """Drive one telemetry update through the tracker the way the worker would."""
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
    tracker.update(
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
    def test_empty_registry_returns_full_shape_with_zero_totals(self):
        """Empty registry must still expose every totals key — SADE Central's
        page reads each status / flag bucket without defending against missing
        keys."""
        snap = build_drone_snapshot(ActiveSessionRegistry(), DroneStateTracker())

        assert set(snap.keys()) == {"report_time_utc", "totals", "drones"}
        assert snap["drones"] == []
        assert snap["totals"]["active_drones"] == 0
        assert snap["totals"]["by_status"] == {
            "EXIT_REQUESTED": 0,
            "FLYING":         0,
            "LANDED":         0,
            "WAITING":        0,
            "ACTIVE":         0,
        }
        assert snap["totals"]["by_flag"] == {"past_deadline": 0, "stranded": 0}

    def test_report_time_utc_is_iso_string(self):
        snap = build_drone_snapshot(ActiveSessionRegistry(), DroneStateTracker())
        assert isinstance(snap["report_time_utc"], str)
        assert snap["report_time_utc"].endswith("+00:00")


# ── Per-drone entry ──────────────────────────────────────────────────────────


class TestDroneEntry:
    def test_session_with_no_telemetry_renders_as_waiting_with_live_null(self):
        """Registration present, no MQTT yet → status=WAITING and live=None."""
        reg = _make_registry(_make_session("f-1"))
        snap = build_drone_snapshot(reg, DroneStateTracker())

        (drone,) = snap["drones"]
        assert drone["status"] == "WAITING"
        assert drone["live"] is None
        assert drone["flags"] == []

    def test_armed_drone_renders_as_flying_with_open_segment(self):
        reg = _make_registry(_make_session("f-1"))
        tracker = DroneStateTracker()
        _populate(tracker, "f-1", armed=True, voltage=16.2, altitude=92.5,
                  last_seen="2026-05-12T17:30:00+00:00")

        (drone,) = build_drone_snapshot(reg, tracker)["drones"]

        assert drone["status"] == "FLYING"
        assert drone["live"]["currently_armed"] is True
        assert drone["live"]["current_segment_started_at"] is not None
        assert drone["live"]["battery_voltage_v"] == 16.2
        assert drone["live"]["position"] == {
            "latitude": 39.77, "longitude": -86.16, "altitude_m": 92.5,
        }

    def test_disarmed_after_flight_renders_as_landed(self):
        reg = _make_registry(_make_session("f-1"))
        tracker = DroneStateTracker()
        _populate(tracker, "f-1", armed=True,  voltage=16.5,
                  last_seen="2026-05-12T17:10:00+00:00")
        _populate(tracker, "f-1", armed=False, voltage=14.8,
                  last_seen="2026-05-12T17:35:00+00:00")

        (drone,) = build_drone_snapshot(reg, tracker)["drones"]

        assert drone["status"] == "LANDED"
        assert drone["live"]["currently_armed"] is False
        assert drone["live"]["current_segment_started_at"] is None

    def test_legacy_firmware_no_armed_field_renders_currently_armed_as_null(self):
        """Older firmware doesn't emit ``status.armed`` — we surface unknown
        rather than guessing."""
        reg = _make_registry(_make_session("f-1"))
        tracker = DroneStateTracker()
        _populate(tracker, "f-1", voltage=16.0, altitude=50.0,
                  last_seen="2026-05-12T17:20:00+00:00")  # no armed kwarg

        (drone,) = build_drone_snapshot(reg, tracker)["drones"]

        assert drone["status"] == "ACTIVE"  # legacy fallback
        assert drone["live"]["currently_armed"] is None
        assert drone["live"]["current_segment_started_at"] is None

    def test_exit_requested_session_renders_as_exit_requested(self):
        reg = _make_registry(
            _make_session("f-1", exit_requested_at="2026-05-12T17:42:00+00:00"),
        )
        tracker = DroneStateTracker()
        _populate(tracker, "f-1", armed=True, voltage=15.5,
                  last_seen="2026-05-12T17:42:30+00:00")

        (drone,) = build_drone_snapshot(reg, tracker)["drones"]

        assert drone["status"] == "EXIT_REQUESTED"
        assert drone["session_window"]["exit_requested_at"] == "2026-05-12T17:42:00+00:00"

    def test_session_window_carries_registration_fields(self):
        reg = _make_registry(_make_session(
            "f-1",
            requested_entry_time="2026-05-12T17:00:00+00:00",
            requested_exit_time="2026-05-12T18:00:00+00:00",
        ))

        (drone,) = build_drone_snapshot(reg, DroneStateTracker())["drones"]

        window = drone["session_window"]
        assert window["requested_entry_time"] == "2026-05-12T17:00:00+00:00"
        assert window["requested_exit_time"]  == "2026-05-12T18:00:00+00:00"
        assert window["exit_requested_at"]    is None
        assert window["registered_at"]        is not None  # auto-stamped


# ── Totals aggregation ──────────────────────────────────────────────────────


class TestTotalsAggregation:
    def test_by_status_counts_each_session_exactly_once(self):
        reg = _make_registry(
            _make_session("f-waiting", drone_id="drone-waiting"),
            _make_session("f-exit",    drone_id="drone-exit",
                          exit_requested_at="2026-05-12T17:42:00+00:00"),
            _make_session("f-flying",  drone_id="drone-flying"),
            _make_session("f-landed",  drone_id="drone-landed"),
        )
        tracker = DroneStateTracker()
        _populate(tracker, "f-flying", armed=True,
                  last_seen="2026-05-12T17:30:00+00:00")
        _populate(tracker, "f-landed", armed=True,
                  last_seen="2026-05-12T17:10:00+00:00")
        _populate(tracker, "f-landed", armed=False,
                  last_seen="2026-05-12T17:35:00+00:00")
        # f-exit also has telemetry — its status is still EXIT_REQUESTED
        # because exit_requested_at trumps everything else.
        _populate(tracker, "f-exit", armed=True,
                  last_seen="2026-05-12T17:42:30+00:00")

        snap = build_drone_snapshot(reg, tracker)

        assert snap["totals"]["active_drones"] == 4
        assert snap["totals"]["by_status"] == {
            "EXIT_REQUESTED": 1,
            "FLYING":         1,
            "LANDED":         1,
            "WAITING":        1,
            "ACTIVE":         0,
        }

    def test_by_flag_counts_each_flag_independently(self):
        """A session can carry both past_deadline AND stranded — each flag
        is counted independently rather than the session being bucketed."""
        reg = _make_registry(
            _make_session(
                "f-1", drone_id="drone-1",
                exit_deadline_breached_at="2026-05-12T18:01:00+00:00",
            ),
            _make_session(
                "f-2", drone_id="drone-2",
                stranded_flagged_at="2026-05-12T17:50:00+00:00",
            ),
            _make_session(
                "f-3", drone_id="drone-3",
                exit_deadline_breached_at="2026-05-12T18:01:00+00:00",
                stranded_flagged_at="2026-05-12T17:50:00+00:00",
            ),
        )

        snap = build_drone_snapshot(reg, DroneStateTracker())

        assert snap["totals"]["by_flag"] == {"past_deadline": 2, "stranded": 2}

    def test_drone_carries_organization_id_and_zone_id(self):
        """SADE Central groups client-side on these fields — confirm they
        survive into the snapshot."""
        reg = _make_registry(_make_session(
            "f-1",
            organization_id="acme-corp",
            sade_zone_id="north-ridge",
        ))

        (drone,) = build_drone_snapshot(reg, DroneStateTracker())["drones"]

        assert drone["organization_id"] == "acme-corp"
        assert drone["sade_zone_id"]    == "north-ridge"
