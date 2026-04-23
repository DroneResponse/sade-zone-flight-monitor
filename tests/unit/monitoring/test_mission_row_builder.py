"""Unit tests for app.monitoring.mission_row_builder and mission_row_schema."""

from __future__ import annotations

from app.monitoring.mission_row_builder import MissionRowBuilder
from app.monitoring.mission_row_schema import ALL_COLUMNS, make_default_mission_row
from app.monitoring.state_tracker import DroneState


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_drone_state(
    *,
    flight_session_id: str = "flight-001",
    drone_id: str = "drone-01",
    first_seen: str = "2026-01-01T12:00:00Z",
    last_seen: str = "2026-01-01T12:45:00Z",
    max_altitude: float | None = 92.0,
    start_position: dict | None = None,
    position: dict | None = None,
    voltage_in: float | None = 16.4,
    voltage_out: float | None = 14.9,
    distance_flown_m: float = 0.0,
    mission_status: str | None = "mission_completed",
    mode: str | None = "AUTO",
) -> DroneState:
    return DroneState(
        flight_session_id=flight_session_id,
        drone_id=drone_id,
        session_source="aws",
        first_seen=first_seen,
        last_seen=last_seen,
        latest_raw_message={},
        latest_parsed_payload={},
        max_altitude=max_altitude,
        start_position=start_position,
        position=position,
        voltage_in=voltage_in,
        voltage_out=voltage_out,
        distance_flown_m=distance_flown_m,
        mission_status=mission_status,
        mode=mode,
        message_count=10,
    )


BUILDER = MissionRowBuilder()


# ── make_default_mission_row() ───────────────────────────────────────────────


class TestMakeDefaultMissionRow:
    def test_has_all_columns(self):
        row = make_default_mission_row()
        for col in ALL_COLUMNS:
            assert col in row, f"Missing column: {col}"

    def test_static_values(self):
        row = make_default_mission_row()

        assert row["encoding"] == "01"
        assert row["record_type"] == "001"
        assert row["session_id"] == ""
        assert row["uav_id"] == ""
        assert row["precipitation"] == "000"
        assert row["battery.voltage_in"] == {}
        assert row["battery.voltage_out"] == {}
        assert row["incidents"] == []
        assert row["flight.start_lat"] is None
        assert row["flight.end_lat"] is None

    def test_returns_new_dict_each_call(self):
        row_a = make_default_mission_row()
        row_b = make_default_mission_row()

        row_a["uav_id"] = "modified"
        assert row_b["uav_id"] == ""


# ── build_row(): identity fields ─────────────────────────────────────────────


class TestBuildRowIdentity:
    def test_uav_id_from_state(self):
        state = _make_drone_state(drone_id="drone-42")
        row = BUILDER.build_row(state)

        assert row["uav_id"] == "drone-42"

    def test_session_id_from_flight_session_id(self):
        state = _make_drone_state(flight_session_id="flight-xyz")
        row = BUILDER.build_row(state)

        assert row["session_id"] == "flight-xyz"

    def test_time_in_and_time_out(self):
        state = _make_drone_state(
            first_seen="2026-03-09T18:00:00Z",
            last_seen="2026-03-09T19:03:00Z",
        )
        row = BUILDER.build_row(state)

        assert row["time_in"] == "2026-03-09T18:00:00Z"
        assert row["time_out"] == "2026-03-09T19:03:00Z"


# ── build_row(): altitude ────────────────────────────────────────────────────


class TestBuildRowAltitude:
    def test_max_altitude_rounded_to_int(self):
        state = _make_drone_state(max_altitude=92.7)
        row = BUILDER.build_row(state)

        assert row["flight.max_alt_asl_m"] == 93

    def test_max_altitude_none_stays_default(self):
        state = _make_drone_state(max_altitude=None)
        row = BUILDER.build_row(state)

        assert row["flight.max_alt_asl_m"] == 0


# ── build_row(): positions ───────────────────────────────────────────────────


class TestBuildRowPositions:
    def test_start_position_fields(self):
        start = {"latitude": 39.77, "longitude": -86.16, "altitude": 110.0}
        state = _make_drone_state(start_position=start)
        row = BUILDER.build_row(state)

        assert row["flight.start_lat"] == 39.77
        assert row["flight.start_lon"] == -86.16
        assert row["flight.start_alt_m"] == 110.0

    def test_end_position_fields(self):
        end = {"latitude": 39.78, "longitude": -86.17, "altitude": 105.0}
        state = _make_drone_state(position=end)
        row = BUILDER.build_row(state)

        assert row["flight.end_lat"] == 39.78
        assert row["flight.end_lon"] == -86.17
        assert row["flight.end_alt_m"] == 105.0

    def test_no_start_position(self):
        state = _make_drone_state(start_position=None)
        row = BUILDER.build_row(state)

        assert row["flight.start_lat"] is None
        assert row["flight.start_lon"] is None
        assert row["flight.start_alt_m"] is None

    def test_no_end_position(self):
        state = _make_drone_state(position=None)
        row = BUILDER.build_row(state)

        assert row["flight.end_lat"] is None
        assert row["flight.end_lon"] is None
        assert row["flight.end_alt_m"] is None


# ── build_row(): voltage ─────────────────────────────────────────────────────


class TestBuildRowVoltage:
    def test_voltage_in_as_dict(self):
        state = _make_drone_state(voltage_in=16.4)
        row = BUILDER.build_row(state)

        assert row["battery.voltage_in"] == {"A": 16.4}

    def test_voltage_out_as_dict(self):
        state = _make_drone_state(voltage_out=14.9)
        row = BUILDER.build_row(state)

        assert row["battery.voltage_out"] == {"A": 14.9}

    def test_voltage_in_none_stays_default(self):
        state = _make_drone_state(voltage_in=None)
        row = BUILDER.build_row(state)

        assert row["battery.voltage_in"] == {}

    def test_voltage_out_none_stays_default(self):
        state = _make_drone_state(voltage_out=None)
        row = BUILDER.build_row(state)

        assert row["battery.voltage_out"] == {}

    def test_voltage_rounded_to_three_decimals(self):
        state = _make_drone_state(voltage_in=16.12345)
        row = BUILDER.build_row(state)

        assert row["battery.voltage_in"] == {"A": 16.123}


# ── build_row(): distance flown ──────────────────────────────────────────────


class TestBuildRowDistanceFlown:
    def test_converts_meters_to_miles(self):
        # 1609.344 m is exactly 1 mile — good canonical value to pin conversion.
        state = _make_drone_state(distance_flown_m=1609.344)
        row = BUILDER.build_row(state)

        assert row["flight.distance_flown_mi"] == 1.0

    def test_zero_distance(self):
        state = _make_drone_state(distance_flown_m=0.0)
        row = BUILDER.build_row(state)

        assert row["flight.distance_flown_mi"] == 0.0

    def test_rounded_to_three_decimals(self):
        # 100 m ≈ 0.062137 mi; output should round to 3 decimals.
        state = _make_drone_state(distance_flown_m=100.0)
        row = BUILDER.build_row(state)

        assert row["flight.distance_flown_mi"] == 0.062


# ── build_row(): mission status and mode ─────────────────────────────────────


class TestBuildRowStatusAndMode:
    def test_entry_decision_from_mission_status(self):
        state = _make_drone_state(mission_status="mission_completed")
        row = BUILDER.build_row(state)

        assert row["entry_decision"] == "mission_completed"

    def test_entry_conditions_from_mode(self):
        state = _make_drone_state(mode="AUTO")
        row = BUILDER.build_row(state)

        assert row["entry_conditions"] == "AUTO"

    def test_entry_decision_none_becomes_empty_string(self):
        state = _make_drone_state(mission_status=None)
        row = BUILDER.build_row(state)

        assert row["entry_decision"] == ""

    def test_entry_conditions_none_becomes_empty_string(self):
        state = _make_drone_state(mode=None)
        row = BUILDER.build_row(state)

        assert row["entry_conditions"] == ""


# ── build_row(): schema completeness ─────────────────────────────────────────


class TestBuildRowCompleteness:
    def test_row_has_all_columns(self):
        state = _make_drone_state()
        row = BUILDER.build_row(state)

        for col in ALL_COLUMNS:
            assert col in row, f"Missing column: {col}"
