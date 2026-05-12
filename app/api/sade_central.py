"""SADE Central integration endpoint.

SADE Central is a separate Flask web app that surfaces drones,
organizations, identities, and zone state from across the broader SADE
system.  Its "Active Drone Registry + States" page polls this endpoint
to populate the live drone-status view from the Flight Monitor's
in-memory state.

The single endpoint is a flat-list snapshot designed for that page's
needs:

  GET /sade-central/drone_snapshot
    Returns every currently-tracked drone session with its live
    telemetry (or ``live: null`` when no MQTT message has arrived yet),
    plus a pre-aggregated ``totals`` block so the page header can
    render summary counts without iterating the list client-side.

The shape is deliberately drone-centric (one row per session) rather
than zone-grouped, because SADE Central does its own grouping /
filtering / sorting client-side.  If filter pushdown becomes useful
later we can add query params; v1 is simple-as-possible.

Wire-level shape is documented in
``docs/MESSAGE_SHAPES.md`` alongside the other I/O surfaces.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends

from app.api.dashboard import classify_session_status, parse_iso_utc
from app.monitoring.active_session_registry import (
    ActiveFlightSession,
    ActiveSessionRegistry,
)
from app.monitoring.state_tracker import DroneState, DroneStateTracker

LOGGER = logging.getLogger(__name__)

# Every status that classify_session_status can return.  Listed here so
# the totals block always exposes the full set of keys (with zeros when
# no session is in that state) — the SADE Central page can then render
# its summary header without defending against missing keys.
_ALL_STATUSES = ("EXIT_REQUESTED", "FLYING", "LANDED", "WAITING", "ACTIVE")

# Every flag the sweeper can stamp.  Same rationale — keys are always
# present so the page's "⚠ N flagged" badge is a straight read.
_ALL_FLAGS = ("past_deadline", "stranded")


# ── Per-drone entry builder ──────────────────────────────────────────────────


def _build_live_block(
    state: DroneState,
    now_dt: datetime,
) -> dict[str, Any]:
    """Build the ``live`` sub-object from accumulated telemetry state."""
    last_seen_dt = parse_iso_utc(state.last_seen)
    seconds_since_last_seen = (
        (now_dt - last_seen_dt).total_seconds() if last_seen_dt is not None else None
    )

    position_block: dict[str, Any] | None = None
    if state.position is not None:
        lat = state.position.get("latitude")
        lon = state.position.get("longitude")
        alt = state.position.get("altitude")
        # Only emit position when at least one coordinate is present —
        # an all-null block is misleading on a map.
        if lat is not None or lon is not None or alt is not None:
            position_block = {
                "latitude":   lat,
                "longitude":  lon,
                "altitude_m": alt,
            }

    # currently_armed and current_segment_started_at are derived from
    # the same FlightSegment list dashboard.py reads.  Drones whose
    # firmware doesn't emit ``status.armed`` have ``armed_field_seen``
    # False and last_armed None — we surface that as ``null`` rather
    # than guessing.
    currently_armed: bool | None
    current_segment_started_at: str | None
    if not state.armed_field_seen:
        currently_armed = None
        current_segment_started_at = None
    else:
        open_segment = (
            state.segments[-1]
            if state.segments and state.segments[-1].time_out_utc is None
            else None
        )
        currently_armed = open_segment is not None
        current_segment_started_at = open_segment.time_in_utc if open_segment else None

    return {
        "last_seen":                  state.last_seen,
        "seconds_since_last_seen":    seconds_since_last_seen,
        "position":                   position_block,
        "battery_voltage_v":          state.voltage_out,
        "distance_flown_m":           float(state.distance_flown_m),
        "currently_armed":            currently_armed,
        "current_segment_started_at": current_segment_started_at,
    }


def _build_drone_entry(
    session: ActiveFlightSession,
    state: DroneState | None,
    now_dt: datetime,
) -> dict[str, Any]:
    """Build one drone entry for the snapshot's ``drones[]`` array."""
    flags: list[str] = []
    if session.exit_deadline_breached_at is not None:
        flags.append("past_deadline")
    if session.stranded_flagged_at is not None:
        flags.append("stranded")

    return {
        "drone_id":          session.drone_id,
        "flight_session_id": session.flight_session_id,
        "pilot_id":          session.pilot_id,
        "organization_id":   session.organization_id,
        "sade_zone_id":      session.sade_zone_id,
        "status":            classify_session_status(session, state),
        "flags":             flags,
        "session_window": {
            "registered_at":        session.registered_at,
            "requested_entry_time": session.requested_entry_time,
            "requested_exit_time":  session.requested_exit_time,
            "exit_requested_at":    session.exit_requested_at,
        },
        "live": _build_live_block(state, now_dt) if state is not None else None,
    }


# ── Top-level snapshot builder ───────────────────────────────────────────────


def build_drone_snapshot(
    reg: ActiveSessionRegistry,
    tracker: DroneStateTracker,
) -> dict[str, Any]:
    """Build the JSON snapshot SADE Central polls.

    Pre-aggregates ``totals.by_status`` and ``totals.by_flag`` in a
    single pass over the registry so SADE Central's page header can
    render summary counts without re-iterating ``drones[]``.

    Pulled out of the route handler so it can be unit-tested without
    standing up FastAPI.
    """
    now_dt = datetime.now(timezone.utc)
    sessions = list(reg.snapshot().values())

    drones: list[dict[str, Any]] = []
    by_status: dict[str, int] = {status: 0 for status in _ALL_STATUSES}
    by_flag: dict[str, int] = {flag: 0 for flag in _ALL_FLAGS}

    for session in sessions:
        state = tracker.get(session.flight_session_id)
        entry = _build_drone_entry(session, state, now_dt)
        drones.append(entry)

        by_status[entry["status"]] = by_status.get(entry["status"], 0) + 1
        for flag in entry["flags"]:
            by_flag[flag] = by_flag.get(flag, 0) + 1

    return {
        "report_time_utc": now_dt.isoformat(),
        "totals": {
            "active_drones": len(drones),
            "by_status":     by_status,
            "by_flag":       by_flag,
        },
        "drones": drones,
    }


# ── FastAPI router ───────────────────────────────────────────────────────────

router = APIRouter(prefix="/sade-central", tags=["sade-central"])


def get_registry_dep() -> ActiveSessionRegistry:
    """Defined as a function so tests can override it via dependency_overrides.

    Imported lazily inside the body to dodge a circular import at module
    load time — same pattern as ``dashboard.get_registry_dep``.
    """
    from app.api.server import registry
    return registry


def get_state_tracker_dep() -> DroneStateTracker:
    """Same — overridable via dependency_overrides for tests."""
    from app.api.server import state_tracker
    return state_tracker


@router.get(
    "/drone_snapshot",
    summary="Drone-centric snapshot consumed by SADE Central's active-drones page",
    response_description=(
        "JSON object with ``report_time_utc``, pre-aggregated ``totals``, "
        "and a flat ``drones[]`` array — one entry per currently-tracked session."
    ),
)
async def get_drone_snapshot(
    reg: ActiveSessionRegistry = Depends(get_registry_dep),
    tracker: DroneStateTracker = Depends(get_state_tracker_dep),
) -> dict[str, Any]:
    """Return the current drone-registry snapshot for SADE Central."""
    return build_drone_snapshot(reg, tracker)
