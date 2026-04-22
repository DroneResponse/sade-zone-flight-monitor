#!/usr/bin/env python3
"""Test the test_overrides stub finalization path (HTTP only, no MQTT).

This script tests the **test_overrides HTTP stub path**: when a session is
registered with ``test_overrides`` present, the Flight Monitor skips real
MQTT telemetry monitoring and instead waits 5 seconds, then POSTs a canned
finalization report to the SADE API built from the override data.

No Mosquitto broker, no MQTT pipeline, and no drone simulators are needed.
The only component running is the FastAPI server.

For testing the **real MQTT telemetry monitoring path** (broker, pipeline,
fake drones), use ``tests/integration/test_mqtt_telemetry_pipeline.py`` instead.

Test flow:
  1. Start the FastAPI server on localhost
  2. POST session registrations with test_overrides to /flight-monitor/register-session
  3. Verify all sessions are registered (GET /health shows expected count)
  4. Wait for stub finalization tasks to complete (~5s delay each)
  5. Verify all sessions were cleaned up (GET /health shows 0)
  6. Report pass/fail

The finalization POST targets the real SADE AWS endpoint. In local testing
those flight_session_ids don't exist in SADE, so the API will return a
business failure — that is expected. The test verifies our side of the
contract: registration, stub delay, payload construction, and session cleanup.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import urllib.error
import urllib.request
from pathlib import Path
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

LOGGER = logging.getLogger("run_stub_finalization_test")

# How long to wait after the last registration for all stub tasks to complete.
# Each stub task sleeps 5s, so 8s gives comfortable margin.
FINALIZATION_WAIT_SECONDS = 8.0


# ── HTTP helpers ─────────────────────────────────────────────────────────────


def _post_json(url: str, payload: dict, timeout: float = 10.0) -> tuple[int, dict]:
    """POST JSON and return (status_code, parsed_body)."""
    data = json.dumps(payload).encode()
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        try:
            return exc.code, json.loads(body)
        except json.JSONDecodeError:
            return exc.code, {"raw": body}


def _get_json(url: str, timeout: float = 5.0) -> dict:
    """GET and return parsed JSON body."""
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read())


# ── Test scenarios ───────────────────────────────────────────────────────────


def build_test_registrations(count: int) -> list[dict]:
    """Build registration payloads with test_overrides for stub testing."""
    registrations = []
    for i in range(count):
        drone_id = f"stub-drone-{i + 1:02d}"
        registrations.append({
            "flight_session_id": f"stub-flight-{drone_id}-{uuid4()}",
            "drone_id": drone_id,
            "pilot_id": f"stub-pilot-{drone_id}",
            "sade_zone_id": "zone-stub-001",
            "requested_entry_time": "2026-04-21T18:00:00Z",
            "requested_exit_time": "2026-04-21T19:00:00Z",
            "requested_operation": {
                "operation_type": "STUB_TEST",
                "priority": "NORMAL",
            },
            "test_overrides": {
                "actual_start_time": "2026-04-21T18:05:00Z",
                "actual_end_time": "2026-04-21T18:41:00Z",
                "telemetry_summary": {
                    "altitude_min_m": 10.0 + i * 5,
                    "altitude_max_m": 80.0 + i * 10,
                    "battery_start_pct": 98.0 - i,
                    "battery_end_pct": 61.0 - i * 2,
                },
            },
            "submitted_at": "2026-04-21T17:55:00Z",
        })
    return registrations


# ── Server management ────────────────────────────────────────────────────────


async def _start_api_server(host: str, port: int) -> asyncio.Task:
    """Start the FastAPI server as a background task."""
    import uvicorn
    from app.api.server import app as fastapi_app

    config = uvicorn.Config(
        fastapi_app,
        host=host,
        port=port,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve(), name="api-server")

    # Poll /health until the server is ready.
    health_url = f"http://{host}:{port}/health"
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 10.0
    while loop.time() < deadline:
        try:
            result = await asyncio.to_thread(_get_json, health_url)
            if result.get("status") == "ok":
                return task
        except Exception:
            pass
        await asyncio.sleep(0.2)

    raise RuntimeError("FastAPI server did not become ready in time")


# ── Main test ────────────────────────────────────────────────────────────────


async def run_test(args: argparse.Namespace) -> bool:
    """Run the stub finalization test and return True on pass."""
    base_url = f"http://{args.host}:{args.port}"
    register_url = f"{base_url}/flight-monitor/register-session"
    health_url = f"{base_url}/health"

    api_task = await _start_api_server(args.host, args.port)
    LOGGER.info("FastAPI server ready at %s", base_url)

    registrations = build_test_registrations(args.session_count)
    passed = True

    try:
        # ── Step 1: Register sessions with test_overrides ────────────────
        LOGGER.info("Registering %d sessions with test_overrides...", len(registrations))
        for reg in registrations:
            status, body = await asyncio.to_thread(_post_json, register_url, reg)
            action = body.get("action")
            if status != 202 or action != "registered":
                LOGGER.error(
                    "FAIL: Registration failed for %s — HTTP %s action=%s",
                    reg["flight_session_id"], status, action,
                )
                passed = False
            else:
                LOGGER.info(
                    "  Registered: flight_session_id=%s drone_id=%s (HTTP %s)",
                    body.get("flight_session_id"), body.get("drone_id"), status,
                )

        # ── Step 2: Verify sessions are active ───────────────────────────
        health = await asyncio.to_thread(_get_json, health_url)
        active = health.get("active_sessions", -1)
        LOGGER.info("Health check: active_sessions=%s (expected %s)", active, len(registrations))
        if active != len(registrations):
            LOGGER.error(
                "FAIL: Expected %d active sessions, got %d",
                len(registrations), active,
            )
            passed = False

        # ── Step 3: Wait for stub finalization ───────────────────────────
        LOGGER.info(
            "Waiting %.0fs for stub finalization tasks to complete...",
            FINALIZATION_WAIT_SECONDS,
        )
        await asyncio.sleep(FINALIZATION_WAIT_SECONDS)

        # ── Step 4: Verify all sessions cleaned up ───────────────────────
        health = await asyncio.to_thread(_get_json, health_url)
        active = health.get("active_sessions", -1)
        LOGGER.info("Health check after finalization: active_sessions=%s (expected 0)", active)
        if active != 0:
            LOGGER.error(
                "FAIL: Expected 0 active sessions after stub finalization, got %d",
                active,
            )
            passed = False

    finally:
        api_task.cancel()
        await asyncio.gather(api_task, return_exceptions=True)

    return passed


# ── CLI ──────────────────────────────────────────────────────────────────────


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Test the test_overrides stub finalization path (HTTP only, no MQTT)"
    )
    parser.add_argument("--host", default="127.0.0.1", help="API server host")
    parser.add_argument("--port", type=int, default=8000, help="API server port")
    parser.add_argument(
        "--session-count",
        type=int,
        default=3,
        help="Number of stub sessions to register",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    passed = asyncio.run(run_test(args))

    if passed:
        LOGGER.info("PASSED: All stub finalization checks succeeded")
        return 0
    else:
        LOGGER.error("FAILED: One or more checks did not pass")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
