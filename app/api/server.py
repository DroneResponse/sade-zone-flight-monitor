"""FastAPI server for SADE Flight Monitor session lifecycle.

Primary endpoints:
  POST /flight-monitor/register-session
    Receives session registration commands from the SADE outbox per the
    FLIGHT_MONITOR_CONTRACT.md integration contract.  Once registered, the
    telemetry pipeline's workers begin accepting MQTT messages for that drone.

  POST /flight-monitor/exit-request
    Receives exit notifications from SADE when a drone leaves a zone early
    or a session needs to be closed out.  Triggers finalization with whatever
    telemetry has been accumulated.

Deprecated endpoint (kept for backward compatibility):
  POST /entry-approval
    Original entry-approval webhook. Logs a deprecation warning on every call.
    Will be removed once all callers migrate to the contract endpoint.

─── Standalone (API only, no MQTT pipeline) ───────────────────────────────────
    uvicorn app.api.server:app --host 0.0.0.0 --port 8000 --reload

─── Combined with the telemetry pipeline ──────────────────────────────────────
Pass the module-level ``registry`` and ``state_tracker`` to ``run_pipeline()``
so the API server and the MQTT workers share the same session and telemetry state.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse

from app.monitoring.active_session_registry import ActiveSessionRegistry
from app.monitoring.state_tracker import DroneStateTracker
from app.api.approval_handler import (
    EntryApprovalPayload,
    RegisterSessionPayload,
    process_approval,
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

# ── Shared state ─────────────────────────────────────────────────────────────
# These module-level instances are the single source of truth for active
# sessions and accumulated telemetry state.  Import them and pass to
# run_pipeline() when running both the API server and MQTT pipeline together:
#   from app.api.server import registry, state_tracker
registry: ActiveSessionRegistry = ActiveSessionRegistry()
state_tracker: DroneStateTracker = DroneStateTracker()


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
    from datetime import datetime, timezone

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

        minutes_since_exit = (asyncio.get_running_loop().time() - exit_requested_at) / 60.0

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
            # Reset: we need a full silence period from now.
            continue

        # No new telemetry since the last check.  Has the full silence
        # period elapsed since we last saw telemetry (or since the exit
        # request if no telemetry was ever received)?
        if last_seen_at_previous_check is None and current_last_seen is None:
            # No telemetry was ever received for this session.  Use time
            # since exit request as the silence measure.
            silence_seconds = asyncio.get_running_loop().time() - exit_requested_at
        else:
            # Telemetry was received at some point but has now stopped.
            # We've been through at least one check cycle with no change.
            # Count consecutive silent check intervals.  Since we continue
            # (reset) on every new message, reaching here means we've had
            # at least EXIT_GRACE_CHECK_INTERVAL_SECONDS of silence.
            # Use the loop elapsed time minus when we last saw a change.
            silence_seconds = asyncio.get_running_loop().time() - exit_requested_at
            # More precisely, we know last_seen hasn't changed for at least
            # one check interval.  We'll let the loop accumulate until the
            # full grace period passes.

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

    if captured_state is not None:
        payload = build_finalization_payload(captured_state)
    else:
        now_z = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        entry_time = session.requested_entry_time if session else None
        payload = {
            "flight_session_id": flight_session_id,
            "report_time": now_z,
            "actual_start_time": entry_time or now_z,
            "actual_end_time": now_z,
            "telemetry_summary": {
                "altitude_min_m": 0.0,
                "altitude_max_m": 0.0,
                "battery_start_pct": 0.0,
                "battery_end_pct": 0.0,
                "battery_voltage_start_v": 0.0,
                "battery_voltage_end_v": 0.0,
            },
        }

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


@app.post(
    "/entry-approval",
    summary="[Deprecated] Receive a SADE entry approval event",
    response_description=(
        "A JSON object describing the action taken: "
        "'registered' (session activated), "
        "'ignored' (non-approved decision), or "
        "'rejected' (validation error, e.g. duplicate session)."
    ),
    status_code=200,
    deprecated=True,
)
async def entry_approval(
    payload: EntryApprovalPayload,
    reg: ActiveSessionRegistry = Depends(get_registry),
) -> JSONResponse:
    """Deprecated -- use ``POST /flight-monitor/register-session`` instead.

    This endpoint is kept for backward compatibility during migration.
    """
    try:
        result = process_approval(payload, reg)
    except Exception as exc:
        LOGGER.exception(
            "Unexpected error processing entry approval. "
            "evaluation_series_id=%s",
            payload.evaluation_series_id,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return JSONResponse(content=result)


@app.get("/health", summary="Liveness check")
async def health(reg: ActiveSessionRegistry = Depends(get_registry)) -> dict:
    """Return server status and current active session count."""
    return {
        "status": "ok",
        "active_sessions": reg.count(),
    }
