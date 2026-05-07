"""FastAPI server for SADE Flight Monitor session lifecycle.

Endpoints:
  POST /flight-monitor/register-session
    Receives session registration commands from the SADE outbox per the
    FLIGHT_MONITOR_CONTRACT.md integration contract.  Once registered, the
    telemetry pipeline's workers begin accepting MQTT messages for that drone.

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


async def _run_session_sweeper(reg: ActiveSessionRegistry) -> None:
    """Periodically scan the registry for deadline breaches.

    Runs forever until cancelled (typically by the FastAPI lifespan
    shutdown hook).  An exception inside one iteration is logged but
    does not stop the loop — a stuck registry shouldn't take the whole
    sweeper down.
    """
    LOGGER.info(
        "Session sweeper started: interval=%.0fs",
        SWEEPER_INTERVAL_SECONDS,
    )
    try:
        while True:
            await asyncio.sleep(SWEEPER_INTERVAL_SECONDS)
            try:
                _scan_for_deadline_breaches(reg)
            except Exception:  # noqa: BLE001
                LOGGER.exception("Session sweeper iteration failed")
    except asyncio.CancelledError:
        LOGGER.info("Session sweeper stopped")
        raise


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    """FastAPI lifespan: spawn the session sweeper on startup, cancel on shutdown."""
    sweeper_task = asyncio.create_task(
        _run_session_sweeper(registry),
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

    This is the contract endpoint per FLIGHT_MONITOR_CONTRACT.md.
    Registration implicitly means the session has been approved by the SADE
    decision workflow.  Once registered, MQTT telemetry published by the
    drone will be accepted and tracked by the pipeline workers.

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

    # Capture and remove state.
    captured_state = tracker.pop(flight_session_id)
    session = reg.complete(flight_session_id)

    # ── FLIGHT-SEGMENT CLOSE HOOK ────────────────────────────────────────
    # When arm-state-based segment detection is in place, any segment still
    # open on captured_state must be closed here before the payload is
    # built — close at captured_state.last_seen and tag closed_by="finalize".
    # Same hook applies at the worker-side terminal-message finalize path
    # (the "mission_completed" branch that also calls build_finalization_payload).
    if captured_state is not None:
        payload = build_finalization_payload(captured_state)
    else:
        # No telemetry was ever observed for this session.  Build a minimal
        # contract-valid finalization by running a synthetic DroneState
        # through the normal payload builder — same code path, one shape.
        now_iso = datetime.now(timezone.utc).isoformat()
        entry_time = session.requested_entry_time if session else None
        synthetic_state = DroneState(
            flight_session_id=flight_session_id,
            drone_id=drone_id,
            session_source=session.session_source if session else "aws",
            first_seen=entry_time or now_iso,
            last_seen=now_iso,
            latest_raw_message={},
            latest_parsed_payload={},
        )
        payload = build_finalization_payload(synthetic_state)

    await post_tracker_session_finalized(payload)

    LOGGER.info(
        "Exit finalization complete: flight_session_id=%s had_telemetry=%s",
        flight_session_id,
        captured_state is not None,
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
    """Return server status, active-session counts, and deadline-breach counter."""
    return {
        "status": "ok",
        "active_sessions": reg.count(),
        "sessions_past_deadline": reg.count_past_deadline(),
    }
