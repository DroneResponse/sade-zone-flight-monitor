"""Schema definition for mission summary rows in the new asyncio pipeline.

This module is self-contained and intentionally does not import legacy modules.
It mirrors the legacy schema shape so downstream consumers can keep a familiar
row structure.
"""

from __future__ import annotations

from typing import Any

PROFILE_COLUMNS: list[str] = [
    "encoding",
    "session_id",
    "record_type",
    "pilot_id",
    "uav_id",
    "zone_id",
    "wind_steady_kt",
    "wind_gusts_kt",
    "precipitation",
    "visibility_nm",
    "max_temperature_f",
    "min_temperature_f",
    "time_in",
    "time_out",
    "flight.max_alt_asl_m",
    "flight.distance_flown_mi",
    "payload.total_weight_kg",
    "payload.camera",
    "payload.other",
    "battery.voltage_in",
    "battery.voltage_out",
    "battery.recharge_in_zone",
    "battery.types",
    "incidents",
    "entry_decision",
    "entry_conditions",
]

EXT_COLUMNS: list[str] = [
    "flight.start_lat",
    "flight.start_lon",
    "flight.start_alt_m",
    "flight.end_lat",
    "flight.end_lon",
    "flight.end_alt_m",
]

ALL_COLUMNS: list[str] = PROFILE_COLUMNS + EXT_COLUMNS


def make_default_mission_row() -> dict[str, Any]:
    """Return a schema-complete row populated with safe defaults."""
    row: dict[str, Any] = {
        "encoding": "01",
        "session_id": "",
        "record_type": "001",
        "pilot_id": "",
        "uav_id": "",
        "zone_id": "",
        "wind_steady_kt": 0,
        "wind_gusts_kt": 0,
        "precipitation": "000",
        "visibility_nm": 0,
        "max_temperature_f": 0,
        "min_temperature_f": 0,
        "time_in": "",
        "time_out": "",
        "flight.max_alt_asl_m": 0,
        "flight.distance_flown_mi": 0.0,
        "payload.total_weight_kg": 0.0,
        "payload.camera": "00",
        "payload.other": "000",
        "battery.voltage_in": {},
        "battery.voltage_out": {},
        "battery.recharge_in_zone": False,
        "battery.types": "",
        "incidents": [],
        "entry_decision": "",
        "entry_conditions": "",
        "flight.start_lat": None,
        "flight.start_lon": None,
        "flight.start_alt_m": None,
        "flight.end_lat": None,
        "flight.end_lon": None,
        "flight.end_alt_m": None,
    }

    for key in ALL_COLUMNS:
        row.setdefault(key, None)

    return row
