"""FastAPI server for SADE Flight Monitor session lifecycle.

Payload shapes per SADE_AWS_API_INFORMATION/SADE_CONTRACT.md (authoritative);
the Flight-Monitor-implementation side (status codes, retry, sweeper,
trigger cases) is documented in FLIGHT_MONITOR_CONTRACT.md.

Endpoints:
  POST /flight-monitor/register-session
    Receives session registration commands from the SADE outbox.  Once
    registered, the telemetry pipeline's workers begin accepting MQTT
    messages for that drone.

  POST /flight-monitor/exit-request
    Receives exit notifications from SADE when a drone leaves a zone early
    or a session needs to be closed out.  Triggers finalization with whatever
    telemetry has been accumulated.

─── Standalone (API only, no MQTT pipeline) ───────────────────────────────────
    uvicorn app.api.server:app --host 0.0.0.0 --port 8000 --reload

─── Combined with the telemetry pipeline ──────────────────────────────────────
Pass the module-level ``registry`` and ``state_tracker`` to ``run_pipeline()``
so the API server and the MQTT workers share the same session and telemetry state.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse

from app.monitoring.active_session_registry import ActiveSessionRegistry
from app.monitoring.state_tracker import DroneState, DroneStateTracker
from app.api.approval_handler import (
    RegisterSessionPayload,
    process_register_session,
)
from app.api.exit_handler import ExitRequestPayload, process_exit_request
from app.sending.tracker_finalizer import (
    build_finalization_payload,
    build_stub_finalization_payload,
    post_tracker_session_finalized,
)

LOGGER = logging.getLogger(__name__)

STUB_FINALIZATION_DELAY_SECONDS = 5.0
EXIT_GRACE_PERIOD_SECONDS = 300.0  # 5 minutes of telemetry silence before finalizing
EXIT_GRACE_CHECK_INTERVAL_SECONDS = 30.0  # how often to check for new telemetry
SWEEPER_INTERVAL_SECONDS = 60.0  # how often the periodic sweeper scans the registry
STRANDED_SILENCE_THRESHOLD_SECONDS = 600.0  # 10 min silent telemetry → flag stranded

# Memory-safety backstop: a session that has been carrying a deadline-breach
# or stranded flag for longer than this gets force-closed by the sweeper
# (canonical finalize POST + registry/tracker clear).  24 h is a deliberate
# choice — long enough to avoid prematurely closing a session during a
# transient connectivity blip on a long flight, short enough that the
# memory leak from "session never gets an exit-request" is bounded to a
# day rather than indefinite.  Tunable: 12 h would be more aggressive,
# 48 h more conservative.  Adjust here and update the unit tests' bounds
# checks if you change it.
FORCE_CLOSE_THRESHOLD_SECONDS = 86400.0

# ── Shared state ─────────────────────────────────────────────────────────────
# These module-level instances are the single source of truth for active
# sessions and accumulated telemetry state.  Import them and pass to
# run_pipeline() when running both the API server and MQTT pipeline together:
#   from app.api.server import registry, state_tracker
registry: ActiveSessionRegistry = ActiveSessionRegistry()
state_tracker: DroneStateTracker = DroneStateTracker()


# ── Periodic session sweeper ─────────────────────────────────────────────────
# Background coroutine that scans the registry every SWEEPER_INTERVAL_SECONDS
# looking for sessions whose ``requested_exit_time`` has passed without a
# corresponding exit-request from SADE.  Flagged sessions are stamped with
# ``exit_deadline_breached_at`` (one-shot edge detector) and surfaced via
# /health for observability.  No auto-finalize today — flag-and-observe only.


def _scan_for_deadline_breaches(reg: ActiveSessionRegistry) -> int:
    """One sweep iteration: flag sessions past requested_exit_time.

    Returns the number of sessions newly flagged on this pass (zero when
    nothing changed).  Pulled out of the async loop so it can be unit
    tested without dealing with asyncio.sleep / mocked clocks.

    Skipped for sessions where:
    - ``exit_deadline_breached_at`` is already set (one-shot).
    - ``requested_exit_time`` is unset (SADE didn't supply a deadline).
    - ``exit_requested_at`` is set (SADE has already closed it out).
    - ``requested_exit_time`` doesn't parse as ISO 8601 (logged once;
      better to skip than crash the sweeper for one bad record).
    - ``now <= requested_exit_time`` (still within the authorized window).
    """
    now_dt = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()
    newly_flagged = 0

    for session in list(reg.snapshot().values()):
        if session.exit_deadline_breached_at is not None:
            continue
        if not session.requested_exit_time:
            continue
        if session.exit_requested_at is not None:
            continue

        try:
            deadline_dt = datetime.fromisoformat(session.requested_exit_time)
        except (TypeError, ValueError):
            LOGGER.warning(
                "Skipping deadline-breach check for session with unparseable "
                "requested_exit_time. flight_session_id=%s value=%r",
                session.flight_session_id,
                session.requested_exit_time,
            )
            continue

        # SADE sends UTC ISO timestamps with offset/Z, but be defensive:
        # treat any naive datetime as UTC rather than crashing on compare.
        if deadline_dt.tzinfo is None:
            deadline_dt = deadline_dt.replace(tzinfo=timezone.utc)

        if now_dt <= deadline_dt:
            continue

        session.exit_deadline_breached_at = now_iso
        newly_flagged += 1
        LOGGER.warning(
            "Session past requested_exit_time without exit-request. "
            "flight_session_id=%s drone_id=%s requested_exit_time=%s now=%s",
            session.flight_session_id,
            session.drone_id,
            session.requested_exit_time,
            now_iso,
        )

    return newly_flagged


def _scan_for_stranded_sessions(
    reg: ActiveSessionRegistry,
    tracker: DroneStateTracker,
) -> int:
    """One sweep iteration: flag silent sessions with no exit-request.

    Stranded means: the drone was transmitting (DroneState exists), it has
    now been silent for longer than ``STRANDED_SILENCE_THRESHOLD_SECONDS``,
    and SADE has not sent an exit-request.  Sessions where no telemetry
    has ever arrived are deliberately not flagged here — that case belongs
    to the deadline-breach detector via ``requested_exit_time``, since
    flagging "drone hasn't shown up yet" would false-positive on every
    session before its first MQTT message.

    Returns the number of sessions newly flagged on this pass.

    Skipped for sessions where:
    - ``stranded_flagged_at`` is already set (one-shot edge detector).
    - ``exit_requested_at`` is set (SADE has already closed it out).
    - No DroneState exists for the session yet.
    - ``state.last_seen`` is empty or unparseable as ISO 8601.
    - The silence duration is below the threshold.
    """
    now_dt = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()
    newly_flagged = 0

    for session in list(reg.snapshot().values()):
        if session.stranded_flagged_at is not None:
            continue
        if session.exit_requested_at is not None:
            continue

        state = tracker.get(session.flight_session_id)
        if state is None:
            continue
        if not state.last_seen:
            continue

        try:
            last_seen_dt = datetime.fromisoformat(state.last_seen)
        except (TypeError, ValueError):
            LOGGER.warning(
                "Skipping stranded check for session with unparseable "
                "last_seen. flight_session_id=%s value=%r",
                session.flight_session_id,
                state.last_seen,
            )
            continue

        if last_seen_dt.tzinfo is None:
            last_seen_dt = last_seen_dt.replace(tzinfo=timezone.utc)

        silence_seconds = (now_dt - last_seen_dt).total_seconds()
        if silence_seconds < STRANDED_SILENCE_THRESHOLD_SECONDS:
            continue

        session.stranded_flagged_at = now_iso
        newly_flagged += 1
        LOGGER.warning(
            "Session stranded — telemetry silent for %.1f minutes, no exit-request. "
            "flight_session_id=%s drone_id=%s last_seen=%s",
            silence_seconds / 60.0,
            session.flight_session_id,
            session.drone_id,
            state.last_seen,
        )

    return newly_flagged


def _earliest_flag_dt(session) -> datetime | None:
    """Return the earliest sweeper-flag timestamp on a session, or None.

    A session can carry both ``exit_deadline_breached_at`` and
    ``stranded_flagged_at``.  The force-close clock starts at whichever
    fired first, so the 24 h backstop closes earlier when both signals
    have been raised.  Returns None when neither field is set.

    Unparseable timestamps are skipped silently rather than crashing the
    sweeper.  ``_scan_for_stranded_sessions`` already logs a warning the
    first time a malformed timestamp is observed, so we don't double-log
    here.  Naive datetimes are treated as UTC, matching the convention
    elsewhere in this module.
    """
    parsed: list[datetime] = []
    for raw in (session.exit_deadline_breached_at, session.stranded_flagged_at):
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(raw)
        except (TypeError, ValueError):
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        parsed.append(dt)
    return min(parsed) if parsed else None


def _find_force_close_candidates(reg: ActiveSessionRegistry) -> list:
    """Return sessions whose earliest flag is older than the force-close threshold.

    Pulled out of the async scan so it can be unit-tested without async
    scaffolding or HTTP catchers.  Excludes:
    - sessions where SADE has now sent an exit-request (the grace-period
      task owns finalization in that case);
    - sessions with no flags set (nothing to close);
    - sessions whose earliest flag is younger than the threshold.
    """
    now_dt = datetime.now(timezone.utc)
    candidates = []
    for session in list(reg.snapshot().values()):
        if session.exit_requested_at is not None:
            continue
        flag_dt = _earliest_flag_dt(session)
        if flag_dt is None:
            continue
        if (now_dt - flag_dt).total_seconds() < FORCE_CLOSE_THRESHOLD_SECONDS:
            continue
        candidates.append(session)
    return candidates


async def _scan_for_force_close(
    reg: ActiveSessionRegistry,
    tracker: DroneStateTracker,
) -> int:
    """Force-close sessions whose flag has aged past FORCE_CLOSE_THRESHOLD_SECONDS.

    Memory-safety backstop: when a flagged session has been sitting in
    the registry past the configured threshold (default 24 h) without a
    SADE exit-request, we close it ourselves via the canonical finalize
    sequence (``_finalize_session_to_sade``).  Logged at WARNING with
    ``flag=deadline_breach`` or ``flag=stranded`` so the operational
    cause is visible in logs.

    Per-session POST failures are logged but do not abort the loop —
    one stuck session shouldn't block the others from being closed.

    Returns the number of sessions force-closed on this pass.
    """
    candidates = _find_force_close_candidates(reg)
    if not candidates:
        return 0

    closed = 0
    for session in candidates:
        which_flag = (
            "deadline_breach"
            if session.exit_deadline_breached_at is not None
            else "stranded"
        )
        LOGGER.warning(
            "Force-closing session as memory-safety backstop. "
            "flight_session_id=%s drone_id=%s flag=%s "
            "deadline_breached_at=%s stranded_flagged_at=%s",
            session.flight_session_id,
            session.drone_id,
            which_flag,
            session.exit_deadline_breached_at,
            session.stranded_flagged_at,
        )
        try:
            await _finalize_session_to_sade(
                reg,
                tracker,
                session.flight_session_id,
                reason=f"force_close_backstop:{which_flag}",
            )
            closed += 1
        except Exception:  # noqa: BLE001
            LOGGER.exception(
                "Force-close failed for flight_session_id=%s",
                session.flight_session_id,
            )
    return closed


async def _run_session_sweeper(
    reg: ActiveSessionRegistry,
    tracker: DroneStateTracker,
) -> None:
    """Periodically scan the registry for deadline breaches and stranded sessions.

    Runs forever until cancelled (typically by the FastAPI lifespan
    shutdown hook).  An exception inside one iteration is logged but
    does not stop the loop — a stuck registry shouldn't take the whole
    sweeper down.  Both checks run together because they share an
    iteration cadence and both are cheap O(N) over the registry.
    """
    LOGGER.info(
        "Session sweeper started: interval=%.0fs stranded_threshold=%.0fs "
        "force_close_threshold=%.0fs",
        SWEEPER_INTERVAL_SECONDS,
        STRANDED_SILENCE_THRESHOLD_SECONDS,
        FORCE_CLOSE_THRESHOLD_SECONDS,
    )
    try:
        while True:
            await asyncio.sleep(SWEEPER_INTERVAL_SECONDS)
            try:
                _scan_for_deadline_breaches(reg)
                _scan_for_stranded_sessions(reg, tracker)
                # Force-close runs last so any session flagged on this
                # tick gets a full SWEEPER_INTERVAL_SECONDS of grace before
                # it can be force-closed (the threshold check is inclusive
                # but the flag is brand-new this tick, so the math works
                # out to "next tick at the earliest").
                await _scan_for_force_close(reg, tracker)
            except Exception:  # noqa: BLE001
                LOGGER.exception("Session sweeper iteration failed")
    except asyncio.CancelledError:
        LOGGER.info("Session sweeper stopped")
        raise


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    """FastAPI lifespan: spawn the session sweeper on startup, cancel on shutdown."""
    sweeper_task = asyncio.create_task(
        _run_session_sweeper(registry, state_tracker),
        name="session-sweeper",
    )
    try:
        yield
    finally:
        sweeper_task.cancel()
        await asyncio.gather(sweeper_task, return_exceptions=True)


# ── FastAPI app ──────────────────────────────────────────────────────────────
# TODO: Do we want to add authentication to this server? Currently any caller
# that can reach the port can register sessions. Options to consider: API key
# header, mutual TLS, or relying on network-level isolation (e.g. VPC/security
# group) as the auth boundary.
app = FastAPI(
    title="SADE Telemetry Webhook",
    description=(
        "Receives session registration commands from the SADE outbox and "
        "activates drone telemetry monitoring in the pipeline."
    ),
    version="2.0.0",
    lifespan=_lifespan,
)

# Read-only HTML+JSON dashboard at /dashboard.  Imported here (after `app`
# exists so the dashboard module can read this module's registry/state_tracker
# without a circular import at top-level).
from app.api.dashboard import router as _dashboard_router  # noqa: E402

app.include_router(_dashboard_router)

# Drone-snapshot endpoint consumed by SADE Central's active-drones page.
# Same late-import pattern as the dashboard router for the same circular-
# import reason.
from app.api.sade_central import router as _sade_central_router  # noqa: E402

app.include_router(_sade_central_router)


def get_registry() -> ActiveSessionRegistry:
    """FastAPI dependency that provides the shared session registry.

    Defined as a function so it can be overridden in tests:
        app.dependency_overrides[get_registry] = lambda: mock_registry
    """
    return registry


def get_state_tracker() -> DroneStateTracker:
    """FastAPI dependency that provides the shared telemetry state tracker.

    Defined as a function so it can be overridden in tests:
        app.dependency_overrides[get_state_tracker] = lambda: mock_tracker
    """
    return state_tracker


# ── Stub finalization (test_overrides path) ──────────────────────────────────


async def _run_stub_finalization(
    flight_session_id: str,
    test_overrides: dict,
    reg: ActiveSessionRegistry,
) -> None:
    """Wait briefly, then POST a canned finalization report to SADE.

    This replaces the real telemetry monitoring path when ``test_overrides``
    is present on a registered session.  The delay simulates a short flight
    so the SADE workflow sees realistic timing between registration and
    finalization.
    """
    LOGGER.info(
        "Stub finalization scheduled: flight_session_id=%s delay=%.1fs",
        flight_session_id,
        STUB_FINALIZATION_DELAY_SECONDS,
    )

    await asyncio.sleep(STUB_FINALIZATION_DELAY_SECONDS)

    payload = build_stub_finalization_payload(flight_session_id, test_overrides)
    await post_tracker_session_finalized(payload)

    reg.complete(flight_session_id)
    LOGGER.info(
        "Stub finalization complete: flight_session_id=%s",
        flight_session_id,
    )


# ── Endpoints ────────────────────────────────────────────────────────────────


@app.post(
    "/flight-monitor/register-session",
    summary="Register an approved flight session for monitoring",
    response_description=(
        "A JSON object describing the action taken: "
        "'registered' (session activated) or "
        "'rejected' (e.g. duplicate active session for the same drone)."
    ),
    status_code=202,
)
async def register_session(
    payload: RegisterSessionPayload,
    reg: ActiveSessionRegistry = Depends(get_registry),
) -> JSONResponse:
    """Register a SADE-approved flight session for telemetry monitoring.

    Payload shape per SADE_CONTRACT.md.  Registration implicitly means
    the session has been approved by the SADE decision workflow.  Once
    registered, MQTT telemetry published by the drone will be accepted
    and tracked by the pipeline workers.

    When ``test_overrides`` is present, the real MQTT telemetry path is
    bypassed.  Instead, a background task waits 5 seconds and then POSTs a
    canned finalization report built from the override data.
    """
    try:
        result = process_register_session(payload, reg)
    except Exception as exc:
        LOGGER.exception(
            "Unexpected error registering session. flight_session_id=%s",
            payload.flight_session_id,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    # If registration succeeded and test_overrides is present, schedule the
    # stub finalization instead of waiting for real MQTT telemetry.
    if result.get("action") == "registered" and payload.test_overrides is not None:
        asyncio.create_task(
            _run_stub_finalization(
                payload.flight_session_id,
                payload.test_overrides,
                reg,
            ),
            name=f"stub-finalize-{payload.flight_session_id}",
        )

    status_code = 202 if result.get("action") == "registered" else 409
    return JSONResponse(content=result, status_code=status_code)


# ── Canonical SADE finalize sequence ─────────────────────────────────────────


async def _finalize_session_to_sade(
    reg: ActiveSessionRegistry,
    tracker: DroneStateTracker,
    flight_session_id: str,
    *,
    reason: str,
) -> bool:
    """Run the canonical finalize sequence: capture state, clear, build, POST.

    Used by both the exit-grace-period handler and the force-close
    backstop sweeper.  Centralises:
      1. ``tracker.pop`` + ``reg.complete`` to remove session state
      2. Synthetic DroneState fallback when no telemetry was observed
         (so the SADE payload still satisfies the contract)
      3. ``build_finalization_payload`` (one source of truth for shape)
      4. ``post_tracker_session_finalized`` with retry/backoff

    ``reason`` shows up in the log line so we can tell the call sites
    apart when grepping (e.g. ``exit_grace_elapsed`` vs
    ``force_close_backstop``).

    Returns True if telemetry was observed (real DroneState used),
    False if a synthetic minimal payload was built.  Returns False
    silently when neither registry nor tracker had the session — that
    means another path beat us to it (e.g. concurrent grace task) and
    the caller should not treat it as an error.
    """
    captured_state = tracker.pop(flight_session_id)
    session = reg.complete(flight_session_id)
    if captured_state is None and session is None:
        return False

    # build_finalization_payload auto-closes any still-open FlightSegment at
    # captured_state.last_seen — no caller-side close needed.
    if captured_state is not None:
        payload = build_finalization_payload(captured_state)
        had_telemetry = True
    else:
        # No telemetry was ever observed for this session.  Build a minimal
        # contract-valid finalization by running a synthetic DroneState
        # through the normal payload builder — same code path, one shape.
        now_iso = datetime.now(timezone.utc).isoformat()
        entry_time = session.requested_entry_time if session else None
        synthetic_state = DroneState(
            flight_session_id=flight_session_id,
            drone_id=session.drone_id if session else None,
            session_source=session.session_source if session else "aws",
            first_seen=entry_time or now_iso,
            last_seen=now_iso,
            latest_raw_message={},
            latest_parsed_payload={},
        )
        payload = build_finalization_payload(synthetic_state)
        had_telemetry = False

    await post_tracker_session_finalized(payload)

    LOGGER.info(
        "Session finalized to SADE: flight_session_id=%s reason=%s had_telemetry=%s",
        flight_session_id,
        reason,
        had_telemetry,
    )
    return had_telemetry


# ── Exit grace period monitor (exit-request path) ────────────────────────────


async def _run_exit_grace_period(
    flight_session_id: str,
    drone_id: str | None,
    reg: ActiveSessionRegistry,
    tracker: DroneStateTracker,
) -> None:
    """Monitor for telemetry silence after an exit request, then finalize.

    The session stays active during the grace period so MQTT telemetry
    continues to be processed by the pipeline workers.  This task checks
    every ``EXIT_GRACE_CHECK_INTERVAL_SECONDS`` whether new telemetry has
    arrived.  If the drone keeps transmitting, the silence timer resets and
    a warning is logged each cycle.  Once no new messages arrive for
    ``EXIT_GRACE_PERIOD_SECONDS``, the session is finalized and cleaned up.

    If the session disappears during the grace period (e.g. the worker
    finalized it because the drone sent a terminal MQTT status), this task
    exits cleanly.
    """
    LOGGER.info(
        "Exit grace period started: flight_session_id=%s drone_id=%s "
        "silence_threshold=%.0fs check_interval=%.0fs",
        flight_session_id,
        drone_id,
        EXIT_GRACE_PERIOD_SECONDS,
        EXIT_GRACE_CHECK_INTERVAL_SECONDS,
    )

    exit_requested_at = asyncio.get_running_loop().time()
    last_seen_at_previous_check: str | None = None
    # Monotonic timestamp of the most recent check that observed a fresh
    # telemetry update.  Stays None until a change is observed; the silence
    # calculation below falls back to ``exit_requested_at`` in that case so
    # the no-telemetry-ever path keeps its existing behavior.
    last_telemetry_change_monotonic: float | None = None

    while True:
        await asyncio.sleep(EXIT_GRACE_CHECK_INTERVAL_SECONDS)

        # If the session was already finalized by the worker (terminal MQTT
        # status), there is nothing left to do.
        session = reg.get_by_flight_session_id(flight_session_id)
        if session is None:
            LOGGER.info(
                "Exit grace period ending — session already finalized by pipeline. "
                "flight_session_id=%s",
                flight_session_id,
            )
            return

        # Check the latest telemetry timestamp.
        current_state = tracker.get(flight_session_id)
        current_last_seen = current_state.last_seen if current_state else None

        now_monotonic = asyncio.get_running_loop().time()
        minutes_since_exit = (now_monotonic - exit_requested_at) / 60.0

        # If telemetry is still arriving, warn and reset the silence timer.
        if current_last_seen is not None and current_last_seen != last_seen_at_previous_check:
            LOGGER.warning(
                "Drone still transmitting after exit request. "
                "flight_session_id=%s drone_id=%s "
                "minutes_since_exit=%.1f last_telemetry=%s",
                flight_session_id,
                drone_id,
                minutes_since_exit,
                current_last_seen,
            )
            last_seen_at_previous_check = current_last_seen
            # Reset the silence reference clock — we need a full silence
            # period from now, not from the original exit-request time.
            last_telemetry_change_monotonic = now_monotonic
            continue

        # Silence reference clock: the last time we observed a telemetry
        # change, or the exit-request time if no telemetry has ever been
        # seen.  This keeps the grace window measured from when the drone
        # actually went silent rather than from when the exit request
        # arrived (which would prematurely finalize a still-transmitting
        # drone shortly after EXIT_GRACE_PERIOD_SECONDS has elapsed).
        silence_reference = (
            last_telemetry_change_monotonic
            if last_telemetry_change_monotonic is not None
            else exit_requested_at
        )
        silence_seconds = now_monotonic - silence_reference

        if silence_seconds >= EXIT_GRACE_PERIOD_SECONDS:
            break

    # ── Grace period elapsed — finalize ──────────────────────────────────
    LOGGER.info(
        "Exit grace period elapsed — finalizing. flight_session_id=%s drone_id=%s",
        flight_session_id,
        drone_id,
    )

    await _finalize_session_to_sade(
        reg, tracker, flight_session_id, reason="exit_grace_elapsed",
    )


@app.post(
    "/flight-monitor/exit-request",
    summary="Handle a drone exit notification from SADE",
    response_description=(
        "A JSON object describing the action taken: "
        "'accepted' (session found, grace period started) or "
        "'not_found' (no active session for this flight_session_id)."
    ),
    status_code=202,
)
async def exit_request(
    payload: ExitRequestPayload,
    reg: ActiveSessionRegistry = Depends(get_registry),
    tracker: DroneStateTracker = Depends(get_state_tracker),
) -> JSONResponse:
    """Handle an exit notification when a drone leaves a zone early.

    SADE sends this when it detects a drone has departed the zone before
    the authorized window ended.  The session stays active for a grace
    period (5 minutes of telemetry silence) so the Flight Monitor can
    capture the drone's final telemetry as it leaves.  Once silence is
    confirmed, the session is finalized and a report is POSTed to SADE.

    If the drone keeps transmitting after the exit request, a warning is
    logged every 30 seconds until it goes silent.
    """
    try:
        result = process_exit_request(payload, reg, tracker)
    except Exception as exc:
        LOGGER.exception(
            "Unexpected error processing exit request. flight_session_id=%s",
            payload.flight_session_id,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if result["action"] == "accepted":
        asyncio.create_task(
            _run_exit_grace_period(
                payload.flight_session_id,
                result.get("drone_id"),
                reg,
                tracker,
            ),
            name=f"exit-grace-{payload.flight_session_id}",
        )
        return JSONResponse(content=result, status_code=202)

    # Session not found — return 404.
    return JSONResponse(content=result, status_code=404)


@app.get("/health", summary="Liveness check")
async def health(reg: ActiveSessionRegistry = Depends(get_registry)) -> dict:
    """Return server status, active-session count, and sweeper flag counters."""
    return {
        "status": "ok",
        "active_sessions": reg.count(),
        "sessions_past_deadline": reg.count_past_deadline(),
        "sessions_stranded": reg.count_stranded(),
    }
