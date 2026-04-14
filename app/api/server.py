"""FastAPI webhook server for SADE entry approval events.

This server receives POST /entry-approval events (pushed by SADE or a proxy)
and registers approved drone sessions into the shared ActiveSessionRegistry.
Once a session is registered, the telemetry pipeline's workers will begin
accepting MQTT messages published by that drone.

─── Standalone (API only, no MQTT pipeline) ───────────────────────────────────
Run from the sade/ project root:

    uvicorn app.api.server:app --host 0.0.0.0 --port 8000 --reload

─── Combined with the telemetry pipeline ──────────────────────────────────────
Pass the module-level `registry` to run_pipeline() so both the API server and
the MQTT workers share the same session state:

    from app.api.server import app, registry
    import argparse, asyncio, uvicorn
    from main import run_pipeline

    args = argparse.Namespace(
        broker="localhost", port=1883, topic="update_drone",
        out="mission_rows.csv", queue_size=10000, workers=1,
        shutdown_timeout=5, idle_warning_seconds=300,
        session_source_mode="aws", metrics_log_interval=30,
        session_registry=registry,   # <── shared instance
    )

    async def run_both():
        pipeline_task = asyncio.create_task(run_pipeline(args))
        config = uvicorn.Config(app, host="0.0.0.0", port=8000)
        server = uvicorn.Server(config)
        await server.serve()          # blocks until Ctrl-C
        pipeline_task.cancel()

    asyncio.run(run_both())
"""

from __future__ import annotations

import logging

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse



from app.monitoring.active_session_registry import ActiveSessionRegistry
from app.api.approval_handler import EntryApprovalPayload, process_approval

LOGGER = logging.getLogger(__name__)

# ── Shared registry ──────────────────────────────────────────────────────────
# This module-level instance is the single source of truth for active sessions.
# Import it from here and pass it to run_pipeline() when running both together:
#   from app.api.server import registry
registry: ActiveSessionRegistry = ActiveSessionRegistry()


# ── FastAPI app ──────────────────────────────────────────────────────────────
app = FastAPI(
    title="SADE Telemetry Webhook",
    description=(
        "Receives entry approval events from SADE and activates drone session "
        "tracking in the telemetry pipeline."
    ),
    version="1.0.0",
)


def get_registry() -> ActiveSessionRegistry:
    """FastAPI dependency that provides the shared session registry.

    Defined as a function so it can be overridden in tests:
        app.dependency_overrides[get_registry] = lambda: mock_registry
    """
    return registry


# ── Endpoint ─────────────────────────────────────────────────────────────────

@app.post(
    "/entry-approval",
    summary="Receive a SADE entry approval event",
    response_description=(
        "A JSON object describing the action taken: "
        "'registered' (session activated), "
        "'ignored' (non-approved decision), or "
        "'rejected' (validation error, e.g. duplicate session)."
    ),
    status_code=200,
)
async def entry_approval(
    payload: EntryApprovalPayload,
    reg: ActiveSessionRegistry = Depends(get_registry),
) -> JSONResponse:
    """Receive an entry approval event and activate tracking for the drone.

    This endpoint is intentionally lightweight:
    - It validates the incoming JSON via the EntryApprovalPayload Pydantic model.
    - It delegates all state mutation to process_approval() in approval_handler.py.
    - It never blocks on I/O or long-running work.

    On APPROVED decisions the drone's session is registered in the
    ActiveSessionRegistry.  From that point on, MQTT telemetry messages
    published by that drone_id will be accepted and tracked by the pipeline
    workers running in workers.py.

    On non-approved decisions (DENIED, ACTION_REQUIRED, etc.) the event is
    logged and the request returns successfully with action='ignored'.
    """
    try:
        result = process_approval(payload, reg)
    except Exception as exc:
        # Unexpected errors should not crash the server; surface them as 500.
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
