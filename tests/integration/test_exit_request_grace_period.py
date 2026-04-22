#!/usr/bin/env python3
"""Test the exit-request grace period lifecycle (HTTP only, no MQTT).

This script tests the **exit-request grace period path**: when SADE notifies
the Flight Monitor that a drone is leaving a zone early, the system keeps
monitoring for 5 minutes of telemetry silence before finalizing.

No Mosquitto broker, no MQTT pipeline, and no drone simulators are needed.
The only component running is the FastAPI server with shortened grace period
constants so the test completes quickly.

For testing the **real MQTT telemetry monitoring path**, use
``tests/integration/test_mqtt_telemetry_pipeline.py``.

For testing the **test_overrides stub path**, use
``tests/integration/test_stub_finalization_override.py``.

Scenarios tested:
  1. Basic grace period — register, exit, wait for silence, verify cleanup
  2. Session not found — exit request for unknown session returns 404
  3. Natural completion during grace — worker finalizes session mid-grace,
     grace task detects it and exits cleanly
  4. Multiple simultaneous exits — several grace periods run independently
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

LOGGER = logging.getLogger("run_exit_grace_period_test")

# Shortened grace period for testing (real values: 300s / 30s).
TEST_GRACE_PERIOD_SECONDS = 10.0
TEST_GRACE_CHECK_INTERVAL_SECONDS = 3.0

# How long to wait after the grace period should have expired.
GRACE_BUFFER_SECONDS = 5.0


# ── HTTP helpers ─────────────────────────────────────────────────────────────


def _post_json(url: str, payload: dict, timeout: float = 10.0) -> tuple[int, dict]:
    data = json.dumps(payload).encode()
    request = urllib.request.Request(
        url, data=data,
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
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read())


# ── Server management ────────────────────────────────────────────────────────


async def _start_api_server(host: str, port: int) -> asyncio.Task:
    import uvicorn
    import app.api.server as srv

    # Patch grace period constants for fast testing.
    srv.EXIT_GRACE_PERIOD_SECONDS = TEST_GRACE_PERIOD_SECONDS
    srv.EXIT_GRACE_CHECK_INTERVAL_SECONDS = TEST_GRACE_CHECK_INTERVAL_SECONDS

    config = uvicorn.Config(srv.app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve(), name="api-server")

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


# ── Test helpers ─────────────────────────────────────────────────────────────


async def _register_session(base_url: str, flight_session_id: str, drone_id: str) -> bool:
    status, body = await asyncio.to_thread(
        _post_json,
        f"{base_url}/flight-monitor/register-session",
        {
            "flight_session_id": flight_session_id,
            "drone_id": drone_id,
            "pilot_id": f"pilot-{drone_id}",
        },
    )
    return status == 202 and body.get("action") == "registered"


async def _send_exit_request(base_url: str, flight_session_id: str) -> tuple[int, dict]:
    return await asyncio.to_thread(
        _post_json,
        f"{base_url}/flight-monitor/exit-request",
        {
            "flight_session_id": flight_session_id,
            "reason": "drone_left_early",
        },
    )


async def _get_active_sessions(base_url: str) -> int:
    health = await asyncio.to_thread(_get_json, f"{base_url}/health")
    return health.get("active_sessions", -1)


# ── Scenarios ────────────────────────────────────────────────────────────────


async def scenario_basic_grace_period(base_url: str) -> bool:
    """Scenario 1: Register, exit, wait for silence, verify cleanup."""
    LOGGER.info("── Scenario 1: Basic grace period ──")

    fid = f"grace-basic-{uuid4()}"
    ok = await _register_session(base_url, fid, "drone-basic")
    if not ok:
        LOGGER.error("  FAIL: Registration failed")
        return False

    status, body = await _send_exit_request(base_url, fid)
    if status != 202 or body.get("action") != "accepted":
        LOGGER.error("  FAIL: Exit request not accepted (HTTP %s action=%s)", status, body.get("action"))
        return False
    LOGGER.info("  Exit request accepted")

    # Session should still be active during grace period.
    active = await _get_active_sessions(base_url)
    if active < 1:
        LOGGER.error("  FAIL: Expected session active during grace period, got %d", active)
        return False
    LOGGER.info("  Session still active during grace period (active=%d)", active)

    # Wait for grace period + buffer.
    wait = TEST_GRACE_PERIOD_SECONDS + GRACE_BUFFER_SECONDS
    LOGGER.info("  Waiting %.0fs for grace period to expire...", wait)
    await asyncio.sleep(wait)

    active = await _get_active_sessions(base_url)
    if active != 0:
        LOGGER.error("  FAIL: Expected 0 active sessions after grace period, got %d", active)
        return False

    LOGGER.info("  PASS: Session finalized after grace period")
    return True


async def scenario_not_found(base_url: str) -> bool:
    """Scenario 2: Exit request for unknown session returns 404."""
    LOGGER.info("── Scenario 2: Session not found ──")

    status, body = await _send_exit_request(base_url, f"nonexistent-{uuid4()}")
    if status != 404 or body.get("action") != "not_found":
        LOGGER.error("  FAIL: Expected 404 not_found, got HTTP %s action=%s", status, body.get("action"))
        return False

    LOGGER.info("  PASS: Unknown session returned 404")
    return True


async def scenario_natural_completion(base_url: str) -> bool:
    """Scenario 3: Worker finalizes session during grace period."""
    LOGGER.info("── Scenario 3: Natural completion during grace ──")

    from app.api.server import registry

    fid = f"grace-natural-{uuid4()}"
    ok = await _register_session(base_url, fid, "drone-natural")
    if not ok:
        LOGGER.error("  FAIL: Registration failed")
        return False

    status, body = await _send_exit_request(base_url, fid)
    if status != 202:
        LOGGER.error("  FAIL: Exit request not accepted")
        return False
    LOGGER.info("  Exit request accepted, grace period running")

    # Simulate the telemetry worker finalizing the session mid-grace.
    await asyncio.sleep(2.0)
    registry.complete(fid)
    LOGGER.info("  Simulated worker completion (registry.complete called)")

    # Wait for the grace task to detect the session is gone.
    await asyncio.sleep(TEST_GRACE_CHECK_INTERVAL_SECONDS + 2.0)

    active = await _get_active_sessions(base_url)
    if active != 0:
        LOGGER.error("  FAIL: Expected 0 active sessions, got %d", active)
        return False

    LOGGER.info("  PASS: Grace period task exited cleanly after natural completion")
    return True


async def scenario_multiple_exits(base_url: str) -> bool:
    """Scenario 4: Multiple grace periods run independently."""
    LOGGER.info("── Scenario 4: Multiple simultaneous exits ──")

    sessions = []
    for i in range(3):
        fid = f"grace-multi-{i + 1}-{uuid4()}"
        drone = f"drone-multi-{i + 1}"
        ok = await _register_session(base_url, fid, drone)
        if not ok:
            LOGGER.error("  FAIL: Registration failed for %s", drone)
            return False
        sessions.append(fid)

    LOGGER.info("  Registered %d sessions", len(sessions))

    for fid in sessions:
        status, body = await _send_exit_request(base_url, fid)
        if status != 202:
            LOGGER.error("  FAIL: Exit request not accepted for %s", fid)
            return False

    LOGGER.info("  All exit requests accepted")

    active = await _get_active_sessions(base_url)
    if active != 3:
        LOGGER.error("  FAIL: Expected 3 active during grace, got %d", active)
        return False
    LOGGER.info("  All 3 sessions active during grace period")

    wait = TEST_GRACE_PERIOD_SECONDS + GRACE_BUFFER_SECONDS
    LOGGER.info("  Waiting %.0fs for all grace periods to expire...", wait)
    await asyncio.sleep(wait)

    active = await _get_active_sessions(base_url)
    if active != 0:
        LOGGER.error("  FAIL: Expected 0 active after grace, got %d", active)
        return False

    LOGGER.info("  PASS: All 3 sessions finalized independently")
    return True


# ── Main ─────────────────────────────────────────────────────────────────────


async def run_test(args: argparse.Namespace) -> bool:
    base_url = f"http://{args.host}:{args.port}"

    api_task = await _start_api_server(args.host, args.port)
    LOGGER.info(
        "FastAPI server ready at %s (grace_period=%.0fs check_interval=%.0fs)",
        base_url,
        TEST_GRACE_PERIOD_SECONDS,
        TEST_GRACE_CHECK_INTERVAL_SECONDS,
    )

    results: list[tuple[str, bool]] = []

    try:
        results.append(("Basic grace period", await scenario_basic_grace_period(base_url)))
        results.append(("Session not found", await scenario_not_found(base_url)))
        results.append(("Natural completion", await scenario_natural_completion(base_url)))
        results.append(("Multiple exits", await scenario_multiple_exits(base_url)))
    finally:
        api_task.cancel()
        await asyncio.gather(api_task, return_exceptions=True)

    LOGGER.info("")
    LOGGER.info("── Summary ──")
    all_passed = True
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        LOGGER.info("  %s: %s", status, name)
        if not passed:
            all_passed = False

    return all_passed


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Test the exit-request grace period lifecycle (HTTP only, no MQTT)",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--log-level", default="INFO",
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
        LOGGER.info("PASSED: All exit grace period checks succeeded")
        return 0
    else:
        LOGGER.error("FAILED: One or more checks did not pass")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
