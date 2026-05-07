"""Live drone-status dashboard.

Read-only HTML page + JSON snapshot endpoint, mounted on the same FastAPI
app as the SADE webhook endpoints.  The page polls ``/dashboard/data``
every ``DASHBOARD_REFRESH_MS / 1000`` seconds and re-renders.

Sources of truth:
- ``ActiveSessionRegistry`` for which sessions are currently registered,
  who they belong to, the zone, and the sweeper-stamped flags
  (exit_deadline_breached_at / stranded_flagged_at).
- ``DroneStateTracker`` for live telemetry — last_seen, position,
  battery, distance flown, and the ``segments`` list driven by
  status.armed transitions (open segment ⇒ FLYING).

Status taxonomy (one per session, computed in ``_build_session_entry``):

    EXIT_REQUESTED  SADE has sent /flight-monitor/exit-request; grace period
                    is running.
    FLYING          DroneState exists and the most recent FlightSegment is
                    open (status.armed is currently True or last seen True).
    LANDED          DroneState exists and the most recent FlightSegment is
                    closed (status.armed transitioned True → False).  Also
                    used for "drone has telemetry but no segments yet
                    because armed_field_seen is True and armed has only
                    been False" — i.e. drone on the ground, hasn't taken
                    off yet.
    WAITING         Registered but no telemetry has arrived yet.
    ACTIVE          Legacy fallback: telemetry present but firmware
                    doesn't emit ``status.armed`` so we can't classify
                    flying / landed.

``flags`` is a list and is independent of ``status``: a FLYING session
can also be ``stranded`` if telemetry has been silent for >10 min, etc.

Note (2026-05-07): no auth on these endpoints, same gap as
/flight-monitor/register-session and /flight-monitor/exit-request.  See
the priority list in the README.  This is acceptable for the current
internal-network deployment posture; do not expose to the public
internet without adding auth first.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse

from app.monitoring.active_session_registry import (
    ActiveFlightSession,
    ActiveSessionRegistry,
)
from app.monitoring.state_tracker import DroneState, DroneStateTracker

LOGGER = logging.getLogger(__name__)

DASHBOARD_REFRESH_MS = 7000  # browser polling cadence


# ── Snapshot builder ─────────────────────────────────────────────────────────


def _parse_iso_utc(value: str | None) -> datetime | None:
    """Parse an ISO 8601 timestamp into a UTC-aware datetime, or None."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _classify_status(session: ActiveFlightSession, state: DroneState | None) -> str:
    """Compute the four-way status badge for a session."""
    if session.exit_requested_at is not None:
        return "EXIT_REQUESTED"
    if state is None:
        return "WAITING"
    if state.segments and state.segments[-1].time_out_utc is None:
        return "FLYING"
    if state.armed_field_seen:
        # Telemetry present, firmware emits armed.  Either the drone has
        # flown and landed (segments has at least one closed entry) or
        # the drone is sitting on the ground without ever arming
        # (segments is empty).  Both render as "landed" for the operator
        # — they're not flying right now.
        return "LANDED"
    # Legacy firmware: telemetry present but no arm-state signal.
    return "ACTIVE"


def _build_session_entry(
    session: ActiveFlightSession,
    state: DroneState | None,
    now_dt: datetime,
) -> dict[str, Any]:
    """Build one session entry for the dashboard JSON."""
    flags: list[str] = []
    if session.exit_deadline_breached_at is not None:
        flags.append("past_deadline")
    if session.stranded_flagged_at is not None:
        flags.append("stranded")

    live: dict[str, Any] | None = None
    if state is not None:
        last_seen_dt = _parse_iso_utc(state.last_seen)
        seconds_since_last_seen = (
            (now_dt - last_seen_dt).total_seconds() if last_seen_dt is not None else None
        )
        position = state.position or {}
        live = {
            "has_telemetry": True,
            "last_seen": state.last_seen,
            "seconds_since_last_seen": seconds_since_last_seen,
            "altitude_m": position.get("altitude"),
            "voltage_v": state.voltage_out,
            "distance_flown_m": state.distance_flown_m,
            "completed_segments": sum(
                1 for s in state.segments if s.time_out_utc is not None
            ),
            "current_segment_open": bool(
                state.segments and state.segments[-1].time_out_utc is None
            ),
        }

    return {
        "flight_session_id": session.flight_session_id,
        "drone_id": session.drone_id,
        "pilot_id": session.pilot_id,
        "registered_at": session.registered_at,
        "requested_entry_time": session.requested_entry_time,
        "requested_exit_time": session.requested_exit_time,
        "exit_requested_at": session.exit_requested_at,
        "exit_deadline_breached_at": session.exit_deadline_breached_at,
        "stranded_flagged_at": session.stranded_flagged_at,
        "status": _classify_status(session, state),
        "flags": flags,
        "live": live,
    }


def build_dashboard_snapshot(
    reg: ActiveSessionRegistry,
    tracker: DroneStateTracker,
) -> dict[str, Any]:
    """Build a JSON-serializable snapshot of current sessions for the dashboard.

    Sessions are grouped by ``sade_zone_id``.  Sessions whose zone is None
    are bucketed under the ``(unspecified)`` group so they still show up.

    The thresholds block lets the page render context for the flags
    ("stranded means silent > 10 min") without hard-coding values in the
    JS.  Numbers come straight from the sweeper-side constants.
    """
    # Imported here (not at module top) to avoid a circular import:
    # server.py imports this module's router, and the constants live in
    # server.py for compatibility with the integration test's monkey-patch.
    from app.api.server import (
        FORCE_CLOSE_THRESHOLD_SECONDS,
        STRANDED_SILENCE_THRESHOLD_SECONDS,
    )

    now_dt = datetime.now(timezone.utc)

    sessions_by_zone: dict[str, list[dict[str, Any]]] = {}
    for session in reg.snapshot().values():
        state = tracker.get(session.flight_session_id)
        zone_key = session.sade_zone_id or "(unspecified)"
        sessions_by_zone.setdefault(zone_key, []).append(
            _build_session_entry(session, state, now_dt)
        )

    zones = [
        {"sade_zone_id": zone_id, "sessions": sessions}
        for zone_id, sessions in sorted(sessions_by_zone.items())
    ]

    return {
        "report_time_utc": now_dt.isoformat(),
        "thresholds": {
            "stranded_silence_seconds": STRANDED_SILENCE_THRESHOLD_SECONDS,
            "force_close_threshold_seconds": FORCE_CLOSE_THRESHOLD_SECONDS,
        },
        "totals": {
            "active_sessions": reg.count(),
            "sessions_past_deadline": reg.count_past_deadline(),
            "sessions_stranded": reg.count_stranded(),
        },
        "zones": zones,
    }


# ── HTML page (vanilla CSS + JS, no build step) ──────────────────────────────

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>SADE Flight Monitor — Zone Dashboard</title>
<style>
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    margin: 0;
    padding: 24px;
    background: #f5f5f7;
    color: #1d1d1f;
    font-size: 14px;
  }
  .header {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    margin-bottom: 20px;
  }
  h1 { margin: 0; font-size: 22px; font-weight: 600; }
  .last-refresh {
    color: #666;
    font-size: 13px;
    font-variant-numeric: tabular-nums;
  }
  .last-refresh.stale { color: #c00; font-weight: 600; }

  .totals {
    display: flex;
    gap: 32px;
    background: white;
    padding: 16px 24px;
    border-radius: 8px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    margin-bottom: 20px;
  }
  .total { display: flex; flex-direction: column; gap: 4px; }
  .total .label {
    color: #666;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    font-weight: 600;
  }
  .total .value { font-size: 28px; font-weight: 600; font-variant-numeric: tabular-nums; }
  .total.warn .value { color: #b58900; }
  .total.alert .value { color: #c00; }

  .zone {
    background: white;
    border-radius: 8px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    margin-bottom: 16px;
    overflow: hidden;
  }
  .zone h2 {
    margin: 0;
    padding: 10px 20px;
    font-size: 13px;
    background: #ececef;
    border-bottom: 1px solid #ddd;
    font-weight: 600;
    letter-spacing: 0.3px;
  }

  table { width: 100%; border-collapse: collapse; }
  th, td {
    padding: 10px 16px;
    text-align: left;
    border-bottom: 1px solid #eee;
    vertical-align: middle;
  }
  tr:last-child td { border-bottom: none; }
  th {
    color: #666;
    font-weight: 500;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    background: #fafafa;
  }
  td.numeric, th.numeric { text-align: right; font-variant-numeric: tabular-nums; }

  tr.flying { background: #f0fff4; }
  tr.flagged { background: #fffceb; }
  tr.alert { background: #fff0f0; }
  tr:hover { background: #f0f7ff; }
  tr.flying:hover { background: #e0f9e6; }
  tr.flagged:hover { background: #fff5d0; }
  tr.alert:hover { background: #ffd9d9; }

  .drone-cell strong { display: block; }
  .session-id {
    font-family: ui-monospace, "SF Mono", Menlo, monospace;
    font-size: 11px;
    color: #888;
    margin-top: 2px;
  }

  .badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    margin-right: 4px;
    vertical-align: middle;
  }
  .badge-flying { background: #1f883d; color: white; }
  .badge-landed { background: #6e7681; color: white; }
  .badge-waiting { background: #b3b3b3; color: white; }
  .badge-exit { background: #b58900; color: white; }
  .badge-active { background: #6e7681; color: white; }
  .badge-flag {
    background: #fff3cd; color: #856404; border: 1px solid #ffeaa0;
  }
  .badge-alert {
    background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb;
  }

  .empty {
    text-align: center;
    color: #888;
    padding: 48px 16px;
    font-style: italic;
  }
  .footer {
    color: #999;
    font-size: 11px;
    text-align: center;
    margin-top: 24px;
  }
</style>
</head>
<body>
  <div class="header">
    <h1>SADE Flight Monitor — Zone Dashboard</h1>
    <div class="last-refresh" id="last-refresh">loading…</div>
  </div>
  <div id="totals"></div>
  <div id="zones"></div>
  <div class="footer">
    Polling every <span id="refresh-secs"></span>s &middot; read-only view of in-memory registry + telemetry tracker.
  </div>

<script>
  const REFRESH_MS = __REFRESH_MS__;
  document.getElementById('refresh-secs').textContent = (REFRESH_MS / 1000);

  function escapeHtml(s) {
    if (s === null || s === undefined) return '';
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function fmtRelative(seconds) {
    if (seconds === null || seconds === undefined) return '—';
    if (seconds < 0) return 'just now';
    if (seconds < 60) return Math.round(seconds) + 's ago';
    const m = seconds / 60;
    if (m < 60) return m.toFixed(m < 10 ? 1 : 0) + 'm ago';
    const h = m / 60;
    return h.toFixed(h < 10 ? 1 : 0) + 'h ago';
  }

  function fmtNumber(v, suffix, digits) {
    if (v === null || v === undefined) return '—';
    return Number(v).toFixed(digits || 0) + (suffix || '');
  }

  function statusBadgeHtml(status) {
    const cls = {
      'FLYING':         'badge-flying',
      'LANDED':         'badge-landed',
      'WAITING':        'badge-waiting',
      'EXIT_REQUESTED': 'badge-exit',
      'ACTIVE':         'badge-active',
    }[status] || 'badge-active';
    const label = status.toLowerCase().replace(/_/g, '-');
    return `<span class="badge ${cls}">${escapeHtml(label)}</span>`;
  }

  function rowClassFor(session) {
    if (session.flags.includes('stranded')) return 'alert';
    if (session.flags.length > 0) return 'flagged';
    if (session.status === 'FLYING') return 'flying';
    return '';
  }

  function renderTotals(t) {
    const html = `
      <div class="totals">
        <div class="total">
          <div class="label">Active sessions</div>
          <div class="value">${t.active_sessions}</div>
        </div>
        <div class="total ${t.sessions_past_deadline > 0 ? 'warn' : ''}">
          <div class="label">Past deadline</div>
          <div class="value">${t.sessions_past_deadline}</div>
        </div>
        <div class="total ${t.sessions_stranded > 0 ? 'alert' : ''}">
          <div class="label">Stranded</div>
          <div class="value">${t.sessions_stranded}</div>
        </div>
      </div>`;
    document.getElementById('totals').innerHTML = html;
  }

  function renderZones(zones) {
    const target = document.getElementById('zones');
    if (zones.length === 0) {
      target.innerHTML = '<div class="zone"><div class="empty">No active sessions.</div></div>';
      return;
    }

    target.innerHTML = zones.map(zone => {
      const rows = zone.sessions.map(s => {
        const flagBadges = s.flags.map(f => {
          const cls = f === 'stranded' ? 'badge-alert' : 'badge-flag';
          const label = f.replace(/_/g, ' ');
          return `<span class="badge ${cls}">${escapeHtml(label)}</span>`;
        }).join('');

        const live = s.live;
        const segCell = live
          ? `${live.completed_segments}${live.current_segment_open ? ' + 1 open' : ''}`
          : '—';

        const tooltip = [
          `flight_session_id: ${s.flight_session_id}`,
          `registered_at: ${s.registered_at || '—'}`,
          `requested_entry_time: ${s.requested_entry_time || '—'}`,
          `requested_exit_time: ${s.requested_exit_time || '—'}`,
          s.exit_requested_at ? `exit_requested_at: ${s.exit_requested_at}` : null,
          s.exit_deadline_breached_at ? `deadline_breached_at: ${s.exit_deadline_breached_at}` : null,
          s.stranded_flagged_at ? `stranded_flagged_at: ${s.stranded_flagged_at}` : null,
        ].filter(x => x !== null).join('\\n');

        const shortFid = s.flight_session_id.length > 12
          ? s.flight_session_id.slice(0, 12) + '…'
          : s.flight_session_id;

        return `
          <tr class="${rowClassFor(s)}" title="${escapeHtml(tooltip)}">
            <td class="drone-cell">
              <strong>${escapeHtml(s.drone_id || '—')}</strong>
              <div class="session-id">${escapeHtml(shortFid)}</div>
            </td>
            <td>${escapeHtml(s.pilot_id || '—')}</td>
            <td>${statusBadgeHtml(s.status)}${flagBadges}</td>
            <td>${live ? escapeHtml(fmtRelative(live.seconds_since_last_seen)) : '—'}</td>
            <td class="numeric">${live ? fmtNumber(live.altitude_m, ' m', 1) : '—'}</td>
            <td class="numeric">${live ? fmtNumber(live.voltage_v, ' V', 2) : '—'}</td>
            <td class="numeric">${live ? fmtNumber(live.distance_flown_m, ' m', 0) : '—'}</td>
            <td class="numeric">${escapeHtml(segCell)}</td>
          </tr>`;
      }).join('');

      return `
        <div class="zone">
          <h2>Zone: ${escapeHtml(zone.sade_zone_id)} &middot; ${zone.sessions.length} session${zone.sessions.length === 1 ? '' : 's'}</h2>
          <table>
            <thead>
              <tr>
                <th>Drone</th>
                <th>Pilot</th>
                <th>Status</th>
                <th>Last seen</th>
                <th class="numeric">Altitude</th>
                <th class="numeric">Voltage</th>
                <th class="numeric">Distance flown</th>
                <th class="numeric">Segments</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </div>`;
    }).join('');
  }

  const refreshLabel = document.getElementById('last-refresh');

  async function refresh() {
    try {
      const r = await fetch('/dashboard/data', { cache: 'no-store' });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const data = await r.json();
      renderTotals(data.totals);
      renderZones(data.zones);
      refreshLabel.textContent = 'last refresh: ' + new Date().toLocaleTimeString();
      refreshLabel.classList.remove('stale');
    } catch (err) {
      refreshLabel.textContent = 'refresh failed (' + err.message + ') — retrying';
      refreshLabel.classList.add('stale');
    }
  }

  refresh();
  setInterval(refresh, REFRESH_MS);
</script>
</body>
</html>
""".replace("__REFRESH_MS__", str(DASHBOARD_REFRESH_MS))


# ── FastAPI router ───────────────────────────────────────────────────────────


router = APIRouter(prefix="/dashboard", tags=["dashboard"])


def get_registry_dep() -> ActiveSessionRegistry:
    """Defined as a function so tests can override it via dependency_overrides."""
    from app.api.server import registry
    return registry


def get_state_tracker_dep() -> DroneStateTracker:
    """Same — overridable via dependency_overrides for tests."""
    from app.api.server import state_tracker
    return state_tracker


@router.get(
    "",
    response_class=HTMLResponse,
    summary="Live drone-status dashboard (HTML page)",
)
async def dashboard_page() -> HTMLResponse:
    """Serve the dashboard HTML.  Static — same string for every request."""
    return HTMLResponse(content=DASHBOARD_HTML)


@router.get(
    "/data",
    summary="Live drone-status dashboard data (JSON snapshot)",
)
async def dashboard_data(
    reg: ActiveSessionRegistry = Depends(get_registry_dep),
    tracker: DroneStateTracker = Depends(get_state_tracker_dep),
) -> dict[str, Any]:
    """Return a fresh snapshot for the dashboard page to render.

    Polled by the dashboard JS every DASHBOARD_REFRESH_MS milliseconds.
    """
    return build_dashboard_snapshot(reg, tracker)
