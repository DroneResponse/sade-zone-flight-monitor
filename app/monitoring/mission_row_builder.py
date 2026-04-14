"""Mission summary row builder for the asyncio telemetry pipeline.

The builder converts the final in-memory state for one tracked session into a
schema-compliant mission summary row. The row format stays the same regardless
of whether the session came from AWS approval or local synthetic testing.
"""

from __future__ import annotations

from app.monitoring.mission_row_schema import make_default_mission_row
from app.monitoring.state_tracker import DroneState


class MissionRowBuilder:
    """Build one schema-compliant mission row from tracked mission state."""

    def build_row(self, state: DroneState) -> dict:
        """Build the final summary row for a completed tracked session."""
        row = make_default_mission_row()
        row["uav_id"] = state.drone_id

        # Reuse the session identifier field for either AWS flight session ids or
        # locally synthesized session ids. This keeps the CSV stable while still
        # preserving the authoritative session correlation key.
        row["session_id"] = state.flight_session_id
        row["time_in"] = state.first_seen
        row["time_out"] = state.last_seen

        if state.max_altitude is not None:
            row["flight.max_alt_asl_m"] = int(round(state.max_altitude))

        start_position = state.start_position or {}
        row["flight.start_lat"] = start_position.get("latitude")
        row["flight.start_lon"] = start_position.get("longitude")
        row["flight.start_alt_m"] = start_position.get("altitude")

        end_position = state.position or {}
        row["flight.end_lat"] = end_position.get("latitude")
        row["flight.end_lon"] = end_position.get("longitude")
        row["flight.end_alt_m"] = end_position.get("altitude")

        if state.voltage_in is not None:
            row["battery.voltage_in"] = {"A": round(float(state.voltage_in), 3)}

        if state.voltage_out is not None:
            row["battery.voltage_out"] = {"A": round(float(state.voltage_out), 3)}

        row["entry_decision"] = str(state.mission_status or "")
        row["entry_conditions"] = str(state.mode or "")
        return row
