"""In-memory live drone mission-state tracker.

This tracker stores the latest observed telemetry state for each *active flight
session*. That distinction matters because the same drone may fly multiple
missions over time, and in AWS-backed mode the authoritative session key is the
approved ``flight_session_id``.

In local mode, the flight session id can be a synthetic session created just for
local testing. Either way, the tracker API stays the same for the worker.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import asin, cos, radians, sin, sqrt
from typing import Any

LOGGER = logging.getLogger(__name__)

# Mean Earth radius, used by the haversine great-circle distance formula.
# Accuracy is ~0.3% vs. the true WGS-84 ellipsoid — well within the tolerance
# expected for a flight-path total-distance metric.
_EARTH_RADIUS_M = 6_371_000.0


@dataclass
class FlightSegment:
    """One armed→disarmed window observed for a session.

    Populated by the arm-state transition logic in ``DroneStateTracker``:
    a new segment opens when ``status.armed`` flips False/None → True,
    and closes when it flips True → False.  Voltage_in is captured at
    arm time; voltage_out is the most recent reading observed during
    the segment (so the disarm message itself doesn't have to carry
    voltage data for us to record a useful out-value).

    A segment with ``time_out_utc=None`` is still open — typically the
    drone is mid-flight.  Auto-closed inside ``build_finalization_payload``
    at finalize time using the session's ``last_seen``.
    """

    time_in_utc: str
    time_out_utc: str | None = None
    voltage_in: float | None = None
    voltage_out: float | None = None


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

    # ── Flight-segment detection ─────────────────────────────────────────
    # Populated by the arm-state transition logic in DroneStateTracker.
    # ``last_armed`` carries the most recent observed value of
    # ``status.armed`` so transitions can be detected on the next message.
    # ``armed_field_seen`` is True the first time we see the field at all
    # (regardless of value); it switches the finalization payload builder
    # from the legacy "one synthetic segment per session" fallback to the
    # per-segment emission path.
    segments: list[FlightSegment] = field(default_factory=list)
    last_armed: bool | None = None
    armed_field_seen: bool = False


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
        armed: bool | None = None,
        last_seen: str | None = None,
    ) -> DroneState:
        """Create or update a session's latest mission state and return it.

        The ``armed`` kwarg is the worker-extracted ``status.armed`` boolean.
        ``None`` means the field was absent (older firmware) — the session
        falls back to the legacy "one synthetic segment per session" payload
        shape at finalize time.
        """
        observed_at = last_seen or datetime.now(timezone.utc).isoformat()
        current_altitude = _safe_float((position or {}).get("altitude"))
        current_voltage = _extract_voltage(parsed_payload)

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
            _handle_arm_state(state, armed, observed_at, current_voltage)
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

        _handle_arm_state(existing, armed, observed_at, current_voltage)

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


def _handle_arm_state(
    state: DroneState,
    armed: bool | None,
    observed_at: str,
    current_voltage: float | None,
) -> None:
    """Apply arm-state transitions to a session's flight-segment list.

    Called from ``DroneStateTracker.update`` after the existing telemetry
    accumulators run.  The transitions are:

      None or absent          → no-op (firmware doesn't emit ``status.armed``)
      None/False → True       → open a new segment at observed_at
      True → True             → update voltage_out (latest reading wins)
      True → False            → close the open segment at observed_at
      False → False           → no-op (drone is on the ground)

    The first time we see ``armed`` is non-None, ``armed_field_seen`` flips
    to True so the payload builder switches off the legacy "one synthetic
    segment" fallback for this session.

    Logs an INFO line on each arm/disarm transition for ops visibility.
    """
    if armed is None:
        return

    state.armed_field_seen = True
    last_armed = state.last_armed

    # Arming transition (False/None → True): open a new segment.
    if armed and last_armed is not True:
        state.segments.append(
            FlightSegment(
                time_in_utc=observed_at,
                voltage_in=current_voltage,
                voltage_out=current_voltage,
            )
        )
        LOGGER.info(
            "Arm-state transition: ARMED. flight_session_id=%s drone_id=%s "
            "segment_index=%d time_in_utc=%s",
            state.flight_session_id,
            state.drone_id,
            len(state.segments) - 1,
            observed_at,
        )

    # Keep voltage_out current on any message that lands while a segment is
    # open — including the disarm message itself (so its voltage is captured
    # before the segment closes).  Robust against disarm messages that
    # happen to arrive without a fresh voltage reading.
    if state.segments and state.segments[-1].time_out_utc is None:
        if current_voltage is not None:
            state.segments[-1].voltage_out = current_voltage

    # Disarming transition (True → False): close the currently-open segment.
    if not armed and last_armed is True:
        if state.segments and state.segments[-1].time_out_utc is None:
            closed = state.segments[-1]
            closed.time_out_utc = observed_at
            LOGGER.info(
                "Arm-state transition: DISARMED. flight_session_id=%s drone_id=%s "
                "segment_index=%d time_in_utc=%s time_out_utc=%s",
                state.flight_session_id,
                state.drone_id,
                len(state.segments) - 1,
                closed.time_in_utc,
                closed.time_out_utc,
            )

    state.last_armed = armed


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
