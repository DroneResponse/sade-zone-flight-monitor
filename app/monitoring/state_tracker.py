"""In-memory live drone mission-state tracker.

This tracker stores the latest observed telemetry state for each *active flight
session*. That distinction matters because the same drone may fly multiple
missions over time, and in AWS-backed mode the authoritative session key is the
approved ``flight_session_id``.

In local mode, the flight session id can be a synthetic session created just for
local testing. Either way, the tracker API stays the same for the worker.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import asin, cos, radians, sin, sqrt
from typing import Any

# Mean Earth radius, used by the haversine great-circle distance formula.
# Accuracy is ~0.3% vs. the true WGS-84 ellipsoid — well within the tolerance
# expected for a flight-path total-distance metric.
_EARTH_RADIUS_M = 6_371_000.0


@dataclass
class DroneState:
    """Represents the current mission summary state for one active session."""

    flight_session_id: str
    drone_id: str
    session_source: str
    first_seen: str
    last_seen: str
    latest_raw_message: dict[str, Any]
    latest_parsed_payload: dict[str, Any]
    mission_status: Any = None
    mode: Any = None
    position: dict[str, Any] | None = None
    start_position: dict[str, Any] | None = None
    max_altitude: float | None = None
    min_altitude: float | None = None
    voltage_in: float | None = None
    voltage_out: float | None = None
    distance_flown_m: float = 0.0
    message_count: int = 0
    row_written: bool = False
    exit_requested_at: str | None = None
    exit_reason: str | None = None

    # FLIGHT-SEGMENT DETECTION (not yet implemented — pending firmware emitting
    # an arm-state field).  When it lands, add here:
    #
    #     segments: list[FlightSegment] = field(default_factory=list)
    #
    # where FlightSegment captures {time_in_utc, time_out_utc, voltage_in,
    # voltage_out, min_altitude, max_altitude, opened_by, closed_by}.
    # Per-segment altitude/voltage accumulators move off DroneState and onto
    # the currently-open segment.  distance_flown_m stays session-level —
    # SADE's telemetry_summary.distance_flown_m is one value, not per-segment.


class DroneStateTracker:
    """Tracks live per-session mission state in memory, keyed by flight session id."""

    def __init__(self) -> None:
        self._states: dict[str, DroneState] = {}

    def update(
        self,
        flight_session_id: str,
        *,
        drone_id: str,
        session_source: str,
        raw_message: dict[str, Any],
        parsed_payload: dict[str, Any],
        mission_status: Any,
        mode: Any,
        position: dict[str, Any] | None,
        last_seen: str | None = None,
    ) -> DroneState:
        """Create or update a session's latest mission state and return it."""
        observed_at = last_seen or datetime.now(timezone.utc).isoformat()
        current_altitude = _safe_float((position or {}).get("altitude"))
        current_voltage = _extract_voltage(parsed_payload)

        # ── FLIGHT-SEGMENT DETECTION HOOK (primary signal) ───────────────────
        # When firmware starts emitting an arm-state field (proposed:
        # parsed_payload["status"]["flight_state"] ∈ {"ARMED", "DISARMED"}),
        # read it here before routing altitude/voltage below:
        #
        #   flight_state = (parsed_payload.get("status") or {}).get("flight_state")
        #   # ARMED with no open segment → open one at observed_at.
        #   # DISARMED with an open segment → close it at observed_at
        #   #   (tag closed_by="DISARMED").
        #   # Absent field → legacy behavior (one segment spanning the session).
        #
        # Once the open segment is identified, altitude/voltage accumulators
        # below should write into segment.{min,max}_altitude and
        # segment.voltage_{in,out} instead of the top-level DroneState fields.
        # distance_flown_m stays session-level.

        existing = self._states.get(flight_session_id)
        if existing is None:
            state = DroneState(
                flight_session_id=flight_session_id,
                drone_id=drone_id,
                session_source=session_source,
                first_seen=observed_at,
                last_seen=observed_at,
                latest_raw_message=raw_message,
                latest_parsed_payload=parsed_payload,
                mission_status=mission_status,
                mode=mode,
                position=position,
                start_position=dict(position) if position is not None else None,
                max_altitude=current_altitude,
                min_altitude=current_altitude,
                voltage_in=current_voltage,
                voltage_out=current_voltage,
                message_count=1,
            )
            self._states[flight_session_id] = state
            return state

        existing.last_seen = observed_at
        existing.latest_raw_message = raw_message
        existing.latest_parsed_payload = parsed_payload
        existing.mission_status = mission_status
        existing.mode = mode

        # Accumulate great-circle distance from the previous GPS fix to the new
        # one before overwriting ``existing.position``.  Raw accumulator — no
        # jitter filtering.  A stationary drone with noisy GPS can add small
        # phantom distances over many samples; accepted as a known tradeoff.
        if position is not None and existing.position is not None:
            prev_lat = _safe_float(existing.position.get("latitude"))
            prev_lon = _safe_float(existing.position.get("longitude"))
            new_lat = _safe_float(position.get("latitude"))
            new_lon = _safe_float(position.get("longitude"))
            if None not in (prev_lat, prev_lon, new_lat, new_lon):
                existing.distance_flown_m += _haversine_m(
                    prev_lat, prev_lon, new_lat, new_lon,
                )

        existing.position = position
        existing.message_count += 1

        if existing.start_position is None and position is not None:
            existing.start_position = dict(position)

        if current_altitude is not None:
            if existing.max_altitude is None:
                existing.max_altitude = current_altitude
            else:
                existing.max_altitude = max(existing.max_altitude, current_altitude)
            if existing.min_altitude is None:
                existing.min_altitude = current_altitude
            else:
                existing.min_altitude = min(existing.min_altitude, current_altitude)

        if current_voltage is not None:
            if existing.voltage_in is None:
                existing.voltage_in = current_voltage
            existing.voltage_out = current_voltage

        return existing

    def get(self, flight_session_id: str) -> DroneState | None:
        """Return state for one flight session, or None if unknown."""
        return self._states.get(flight_session_id)

    def pop(self, flight_session_id: str) -> DroneState | None:
        """Remove and return state for a session when that mission is finalized."""
        return self._states.pop(flight_session_id, None)

    def snapshot(self) -> dict[str, DroneState]:
        """Return a shallow snapshot of all tracked session states."""
        return dict(self._states)

    def count(self) -> int:
        """Return number of active session states currently tracked in memory."""
        return len(self._states)


def _extract_voltage(parsed_payload: dict[str, Any]) -> float | None:
    """Extract battery voltage from common payload locations."""
    status = parsed_payload.get("status") if isinstance(parsed_payload.get("status"), dict) else {}
    battery = status.get("battery") if isinstance(status.get("battery"), dict) else {}

    candidate = battery.get("voltage") if battery else status.get("voltage", parsed_payload.get("voltage"))
    return _safe_float(candidate)


def _safe_float(value: Any) -> float | None:
    """Convert numeric-like values to float; return None if invalid."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two (lat, lon) points, in meters.

    Uses the haversine formula with a spherical Earth approximation.  Accurate
    to ~0.3 % vs. the true WGS-84 ellipsoid — sufficient for aggregate
    flight-path distance.
    """
    lat1r, lat2r = radians(lat1), radians(lat2)
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(lat1r) * cos(lat2r) * sin(dlon / 2) ** 2
    return 2 * _EARTH_RADIUS_M * asin(sqrt(a))
