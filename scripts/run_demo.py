#!/usr/bin/env python3
"""Live, narrated demo of the SADE Flight Monitor system.

Walks the audience through the full lifecycle of two simulated drones,
pausing at each milestone so the presenter can talk and switch to the
dashboard.  Everything runs in this single Python process — there is no
docker-compose, no separate broker, no external API.  All the running
parts share the same in-memory registry + telemetry state, so a human
can see the dashboard reflect events the moment they happen.

What gets demonstrated, in order:

    1.  Service boots: FastAPI server, MQTT pipeline, periodic sweeper,
        in-process SADE catcher (mocking SADE's
        /tracker-session-finalized endpoint so finalize POSTs go
        somewhere visible).
    2.  Two flight sessions registered via POST /flight-monitor/register-session.
    3.  Drone telemetry begins; arm-state transitions (ARMED / DISARMED)
        recorded as FlightSegments.
    4.  Multi-segment flight: Drone-alpha arms → flies → disarms →
        re-arms → flies again.  Two segments will appear in its
        eventual finalize payload.
    5.  Exit-request for Drone-bravo → grace period elapses →
        finalize POST goes to the catcher → catcher pretty-prints it.
    6.  Same flow for Drone-alpha.
    7.  Clean shutdown: tasks cancelled, /health reports zero active.

While the demo runs:

    Open  http://localhost:8000/dashboard  in a browser to watch the
    in-memory state in real time.  The page polls every 7 s.

Usage:

    python scripts/run_demo.py             (interactive — Enter to advance)
    python scripts/run_demo.py --auto      (auto-advance with fixed sleeps)
    python scripts/run_demo.py --help

Prerequisites:

    - Mosquitto running on localhost:1883
      (`brew services start mosquitto` on the demo laptop)
    - Ports 8000 (FastAPI) and 8765 (catcher) free

Timing constants are intentionally short so the demo finishes in a
few minutes.  Production values are 5-10× larger; see
docs/EXIT_POLICY_DESIGN.md for the rationale.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import socket
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from uuid import uuid4

import paho.mqtt.client as mqtt
import urllib.request
import urllib.error

# Ensure imports resolve when run from anywhere.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


# ── Demo configuration ───────────────────────────────────────────────────────
# Short timing constants so the demo runs in minutes, not hours.  Override
# Flight-Monitor module-level constants BEFORE the FastAPI app's lifespan
# spawns the periodic sweeper (which reads them).  Production defaults:
#   EXIT_GRACE_PERIOD_SECONDS = 300, EXIT_GRACE_CHECK_INTERVAL_SECONDS = 30,
#   SWEEPER_INTERVAL_SECONDS = 60, STRANDED_SILENCE_THRESHOLD_SECONDS = 600,
#   FORCE_CLOSE_THRESHOLD_SECONDS = 86400.

DEMO_GRACE_PERIOD_SECONDS = 8.0
DEMO_GRACE_CHECK_INTERVAL_SECONDS = 2.0
DEMO_SWEEPER_INTERVAL_SECONDS = 5.0
DEMO_STRANDED_THRESHOLD_SECONDS = 30.0
DEMO_FORCE_CLOSE_THRESHOLD_SECONDS = 60.0

API_HOST = "127.0.0.1"
API_PORT = 8000
CATCHER_PORT = 8765
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "status_message"

ZONE_ID = "demo-zone-001"
DRONE_A_ID = "drone-alpha"
DRONE_B_ID = "drone-bravo"

PUBLISH_INTERVAL_SECONDS = 2.0  # slow enough to keep the console readable


# ── Console helpers (ANSI colour) ────────────────────────────────────────────

BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RED = "\033[31m"
MAGENTA = "\033[35m"


def banner(title: str) -> None:
    print()
    print(f"{BOLD}{CYAN}{'─' * 72}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'─' * 72}{RESET}")


def step(msg: str) -> None:
    print(f"{DIM}»{RESET} {msg}")


def success(msg: str) -> None:
    print(f"{GREEN}✓ {msg}{RESET}")


def fail(msg: str) -> None:
    print(f"{RED}✗ {msg}{RESET}")


def watch(msg: str) -> None:
    """Tell the audience to look at the dashboard."""
    print(f"{MAGENTA}👁  {msg}{RESET}")


def prompt(msg: str, *, auto: bool, auto_seconds: float = 7.0) -> None:
    if auto:
        print(f"{YELLOW}⏸  {msg}  ({auto_seconds:.0f}s pause){RESET}")
        time.sleep(auto_seconds)
    else:
        try:
            input(f"{YELLOW}⏸  {msg}\n   Press Enter to continue...{RESET}")
        except EOFError:
            pass


# ── In-process SADE catcher ──────────────────────────────────────────────────
# Receives /tracker-session-finalized POSTs from the Flight Monitor and
# prints the payload in a presenter-friendly format.  Returns a 200 + the
# EXITED business-success shape so the Flight Monitor's retry path doesn't
# log spurious errors.


class SadeCatcher:
    """Tiny localhost HTTP catcher that mocks SADE's finalize endpoint."""

    def __init__(self, port: int = CATCHER_PORT) -> None:
        self.port = port
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.received: list[dict] = []

    def start(self) -> None:
        catcher = self

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                body_raw = self.rfile.read(length) if length else b"{}"
                try:
                    body = json.loads(body_raw)
                except json.JSONDecodeError:
                    body = {"error": "non-json", "raw": body_raw.decode(errors="replace")}

                catcher.received.append(body)
                catcher._print_received(body)

                response = {
                    "status": "EXITED",
                    "reason": "Session finalized from tracker report.",
                    "flight_session_id": body.get("flight_session_id", "unknown"),
                    "reputation_record_id": str(uuid4()),
                }
                payload = json.dumps(response).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format, *args):
                # Suppress the default Apache-style log line — we print our
                # own demo-friendly summary in _print_received.
                return

        self._server = HTTPServer((API_HOST, self.port), _Handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True, name="sade-catcher"
        )
        self._thread.start()
        success(f"SADE catcher listening on http://{API_HOST}:{self.port}")

    def _print_received(self, body: dict) -> None:
        """Pretty-print a received finalization payload for the audience."""
        print()
        print(f"{BOLD}{GREEN}┌─ SADE RECEIVED FINALIZATION REPORT ─────────────────────────────{RESET}")
        print(f"{BOLD}{GREEN}│{RESET}  flight_session_id : {body.get('flight_session_id')}")
        print(f"{BOLD}{GREEN}│{RESET}  report_time_utc   : {body.get('report_time_utc')}")
        ts = body.get("telemetry_summary", {})
        print(f"{BOLD}{GREEN}│{RESET}  altitude_min/max  : {ts.get('altitude_min_m')} / {ts.get('altitude_max_m')} m")
        print(f"{BOLD}{GREEN}│{RESET}  distance_flown    : {ts.get('distance_flown_m')} m")
        events = body.get("events", [])
        if events:
            print(f"{BOLD}{GREEN}│{RESET}  events            : {len(events)}")
            for i, ev in enumerate(events, 1):
                ev_type = ev.get("type")
                if ev_type == "FLIGHT_SEGMENT":
                    bs_in = ev.get("battery_state_in", {}).get("slots", [{}])[0]
                    bs_out = ev.get("battery_state_out", {}).get("slots", [{}])[0]
                    print(
                        f"{BOLD}{GREEN}│{RESET}    [{i}] FLIGHT_SEGMENT  {ev.get('time_in_utc')} → {ev.get('time_out_utc')}"
                    )
                    print(
                        f"{BOLD}{GREEN}│{RESET}        battery: {bs_in.get('voltage_v')} V → {bs_out.get('voltage_v')} V"
                    )
                elif ev_type == "EXIT_REQUEST":
                    print(
                        f"{BOLD}{GREEN}│{RESET}    [{i}] EXIT_REQUEST   {ev.get('time_utc')}  reason: {ev.get('reason')}"
                    )
                else:
                    print(f"{BOLD}{GREEN}│{RESET}    [{i}] {ev_type}  {ev}")
        print(f"{BOLD}{GREEN}└─────────────────────────────────────────────────────────────────{RESET}")
        print()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)


# ── Demo drone publisher (publishes status.armed for FLIGHT_SEGMENT detection) ──


class DemoDronePublisher:
    """Tiny MQTT publisher tailored for the demo.

    Differences from local_testing/drone_sim.py's SimulatedDronePublisher:

      - Emits ``status.armed`` (the existing simulator omits it, which
        keeps it on the legacy code path that doesn't exercise the new
        FLIGHT_SEGMENT detection — defeats the point of the demo).
      - Publishes at PUBLISH_INTERVAL_SECONDS (2 s default) instead of
        100 ms so the console isn't a wall of "telemetry-worker-1
        updated drone_id=" lines.
      - Caller drives armed/altitude state directly; no orbit/heading
        simulation needed for a demo.
    """

    def __init__(self, drone_id: str, *, broker: str = MQTT_BROKER, port: int = MQTT_PORT,
                 topic: str = MQTT_TOPIC, base_voltage: float = 16.5,
                 home_lat: float = 39.77, home_lon: float = -86.16) -> None:
        self.drone_id = drone_id
        self.broker = broker
        self.port = port
        self.topic = topic
        self.lat = home_lat
        self.lon = home_lon
        self.altitude_m = 0.0
        self.armed = False
        self.voltage = base_voltage

        self._client = mqtt.Client(client_id=f"demo-{drone_id}-{uuid4().hex[:6]}")
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        self._client.connect(self.broker, self.port, keepalive=30)
        self._client.loop_start()
        self._thread = threading.Thread(
            target=self._publish_loop, daemon=True, name=f"pub-{self.drone_id}"
        )
        self._thread.start()

    def set_armed(self, armed: bool) -> None:
        with self._lock:
            self.armed = armed
            if armed and self.altitude_m < 1.0:
                self.altitude_m = 60.0          # take-off
            elif not armed:
                self.altitude_m = 0.0           # back on the ground

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=PUBLISH_INTERVAL_SECONDS + 1.0)
        self._client.loop_stop()
        self._client.disconnect()

    def _publish_loop(self) -> None:
        i = 0
        while not self._stop.is_set():
            with self._lock:
                armed_now = self.armed
                alt_now = self.altitude_m
                v_now = self.voltage
                # Drift altitude a little while flying so the dashboard
                # voltage / altitude cells visibly change.
                if armed_now:
                    self.altitude_m = alt_now + 5.0 * math.sin(i / 4.0)
                    self.voltage = max(13.0, v_now - 0.05)
                # Drift the GPS slightly so distance_flown_m accumulates.
                self.lat += 0.00002 if armed_now else 0.0
            payload = {
                "uavid": self.drone_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": {
                    "status": "tracking" if armed_now else "STANDBY",
                    "mode": "AUTO" if armed_now else "LOITER",
                    "armed": armed_now,
                    "battery": {"voltage": round(v_now, 3), "level": 0.85},
                    "location": {
                        "latitude": round(self.lat, 6),
                        "longitude": round(self.lon, 6),
                        "altitude": round(alt_now, 1),
                    },
                },
            }
            self._client.publish(self.topic, json.dumps(payload), qos=0)
            i += 1
            self._stop.wait(PUBLISH_INTERVAL_SECONDS)


# ── HTTP helpers ─────────────────────────────────────────────────────────────


def post_json(url: str, payload: dict, timeout: float = 5.0) -> tuple[int, dict]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read())
        except json.JSONDecodeError:
            return exc.code, {}


def get_json(url: str, timeout: float = 5.0) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read())


async def wait_for_health(base_url: str, timeout: float = 15.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        try:
            r = await asyncio.to_thread(get_json, f"{base_url}/health", 1.0)
            if r.get("status") == "ok":
                return
        except Exception:  # noqa: BLE001
            pass
        await asyncio.sleep(0.2)
    raise RuntimeError(f"FastAPI server did not come up at {base_url} within {timeout}s")


# ── Pre-flight + service boot ────────────────────────────────────────────────


def is_port_listening(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0


def preflight() -> None:
    """Sanity-check the environment before starting the demo."""
    if not is_port_listening(MQTT_BROKER, MQTT_PORT):
        fail(
            f"Mosquitto is not listening on {MQTT_BROKER}:{MQTT_PORT}.\n"
            f"   Start it with:  brew services start mosquitto\n"
            f"   (or otherwise launch a broker on the default port)"
        )
        sys.exit(1)
    success(f"Mosquitto reachable at {MQTT_BROKER}:{MQTT_PORT}")

    if is_port_listening(API_HOST, API_PORT):
        fail(
            f"Port {API_PORT} is already in use — the FastAPI server can't bind.\n"
            f"   Find and stop the offending process: lsof -nP -iTCP:{API_PORT} -sTCP:LISTEN"
        )
        sys.exit(1)
    success(f"Port {API_PORT} (FastAPI) is free")

    if is_port_listening(API_HOST, CATCHER_PORT):
        fail(
            f"Port {CATCHER_PORT} is already in use — the SADE catcher can't bind.\n"
            f"   lsof -nP -iTCP:{CATCHER_PORT} -sTCP:LISTEN"
        )
        sys.exit(1)
    success(f"Port {CATCHER_PORT} (SADE catcher) is free")


def patch_demo_constants() -> None:
    """Override the Flight Monitor's timing constants for a fast demo.

    Must be called BEFORE the FastAPI app's lifespan handler runs (which
    spawns the periodic sweeper task).  Our run_demo() does this before
    starting uvicorn.
    """
    import app.api.server as srv

    srv.EXIT_GRACE_PERIOD_SECONDS = DEMO_GRACE_PERIOD_SECONDS
    srv.EXIT_GRACE_CHECK_INTERVAL_SECONDS = DEMO_GRACE_CHECK_INTERVAL_SECONDS
    srv.SWEEPER_INTERVAL_SECONDS = DEMO_SWEEPER_INTERVAL_SECONDS
    srv.STRANDED_SILENCE_THRESHOLD_SECONDS = DEMO_STRANDED_THRESHOLD_SECONDS
    srv.FORCE_CLOSE_THRESHOLD_SECONDS = DEMO_FORCE_CLOSE_THRESHOLD_SECONDS

    step(
        f"Patched timing constants: grace={DEMO_GRACE_PERIOD_SECONDS}s "
        f"check={DEMO_GRACE_CHECK_INTERVAL_SECONDS}s sweeper={DEMO_SWEEPER_INTERVAL_SECONDS}s "
        f"stranded={DEMO_STRANDED_THRESHOLD_SECONDS}s "
        f"force_close={DEMO_FORCE_CLOSE_THRESHOLD_SECONDS}s"
    )


async def start_uvicorn_task(api_port: int) -> asyncio.Task:
    """Spawn uvicorn for the FastAPI app as an asyncio task."""
    import uvicorn
    from app.api.server import app as fastapi_app

    config = uvicorn.Config(
        fastapi_app, host=API_HOST, port=api_port, log_level="warning"
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve(), name="api-server")
    return task


async def start_pipeline_task(api_port: int) -> asyncio.Task:
    """Spawn the MQTT ingestion pipeline as an asyncio task.

    Shares the registry + state_tracker singletons with the FastAPI server
    so registrations from the API are immediately visible to the worker
    and worker-accumulated telemetry is visible to the exit-grace handler.
    """
    import tempfile
    from types import SimpleNamespace

    import app.api.server as srv
    import app.main as ingestion_main

    # MissionCsvWriter requires a writeable path — passing "" crashes the
    # pipeline at startup with FileNotFoundError before any subscribe
    # happens.  Route the CSV to a tempfile so the demo doesn't litter
    # the repo root with mission_rows.csv either.
    csv_path = str(Path(tempfile.gettempdir()) / "sade_demo_mission_rows.csv")

    pipeline_args = SimpleNamespace(
        broker=MQTT_BROKER,
        port=MQTT_PORT,
        topic=MQTT_TOPIC,
        out=csv_path,
        queue_size=1000,
        workers=1,
        shutdown_timeout=2.0,
        idle_warning_seconds=120.0,
        session_source_mode="aws",
        metrics_log_interval=60.0,      # quiet
        memory_sample_interval=0.0,
        log_level="INFO",
        session_registry=srv.registry,
        state_tracker=srv.state_tracker,
        finalize_to_api=True,
    )
    return asyncio.create_task(ingestion_main.run_pipeline(pipeline_args), name="pipeline")


# ── API helpers ──────────────────────────────────────────────────────────────


async def register_session(
    base_url: str, drone_id: str, *, fid: str | None = None
) -> str:
    fid = fid or f"demo-{drone_id}-{uuid4().hex[:8]}"
    payload = {
        "flight_session_id": fid,
        "drone_id": drone_id,
        "pilot_id": f"pilot-of-{drone_id}",
        "sade_zone_id": ZONE_ID,
        "requested_entry_time": datetime.now(timezone.utc).isoformat(),
        # +30 min into the future so the deadline-breach sweeper doesn't
        # flag it during the demo.
        "requested_exit_time": (
            datetime.fromtimestamp(time.time() + 1800, tz=timezone.utc).isoformat()
        ),
    }
    status, body = await asyncio.to_thread(
        post_json, f"{base_url}/flight-monitor/register-session", payload
    )
    if status != 202 or body.get("action") != "registered":
        fail(f"Registration failed for {drone_id}: HTTP {status} body={body}")
        sys.exit(1)
    success(f"Registered  drone_id={drone_id}  flight_session_id={fid}")
    return fid


async def send_exit_request(base_url: str, fid: str) -> None:
    payload = {"flight_session_id": fid, "reason": "drone_left_early"}
    status, body = await asyncio.to_thread(
        post_json, f"{base_url}/flight-monitor/exit-request", payload
    )
    if status != 202:
        fail(f"Exit-request unexpected response: HTTP {status} body={body}")
    else:
        success(f"Exit-request accepted for flight_session_id={fid}")


async def show_health(base_url: str) -> None:
    health = await asyncio.to_thread(get_json, f"{base_url}/health")
    print(f"   /health: {json.dumps(health)}")


# ── Demo orchestration ───────────────────────────────────────────────────────


async def run_demo(args: argparse.Namespace) -> None:
    base_url = f"http://{API_HOST}:{API_PORT}"
    auto = args.auto

    # ── Phase 0: pre-flight ─────────────────────────────────────────────
    banner("PHASE 0  Pre-flight")
    preflight()

    # ── Phase 1: boot ───────────────────────────────────────────────────
    banner("PHASE 1  Boot the SADE Flight Monitor")

    # Catcher must be up before TRACKER_FINALIZED_URL is set, since the
    # pipeline pre-flight check at startup hits get_tracker_finalized_url().
    catcher = SadeCatcher()
    catcher.start()

    os.environ["TRACKER_FINALIZED_URL"] = (
        f"http://{API_HOST}:{CATCHER_PORT}/tracker-session-finalized"
    )
    step(f"TRACKER_FINALIZED_URL → {os.environ['TRACKER_FINALIZED_URL']}")

    patch_demo_constants()

    api_task = await start_uvicorn_task(API_PORT)
    await wait_for_health(base_url)
    success(f"FastAPI server up at {base_url}")

    pipeline_task = await start_pipeline_task(API_PORT)
    # Give the pipeline a moment to subscribe to MQTT.
    await asyncio.sleep(2.0)
    success(f"Pipeline subscribed to MQTT topic '{MQTT_TOPIC}' on {MQTT_BROKER}:{MQTT_PORT}")

    watch(f"OPEN A BROWSER:  {BOLD}{base_url}/dashboard{RESET}{MAGENTA}  — empty for now")
    prompt("Boot complete — verify dashboard is reachable.", auto=auto)

    # ── Phase 2: register two sessions ──────────────────────────────────
    banner("PHASE 2  Register two flight sessions via /flight-monitor/register-session")
    step("Sending POST /flight-monitor/register-session for drone-alpha ...")
    fid_a = await register_session(base_url, DRONE_A_ID)
    step("Sending POST /flight-monitor/register-session for drone-bravo ...")
    fid_b = await register_session(base_url, DRONE_B_ID)
    await show_health(base_url)
    watch("Dashboard refreshes every 7 s.  Both sessions should appear in WAITING (no telemetry yet).")
    prompt("Both sessions in registry — see WAITING badges on the dashboard.", auto=auto)

    # ── Phase 3: drone-alpha arms and flies ─────────────────────────────
    banner(f"PHASE 3  {DRONE_A_ID} arms and starts publishing telemetry")
    drone_a = DemoDronePublisher(DRONE_A_ID, base_voltage=16.5)
    drone_a.start()
    step("Publisher connected.  status.armed=False initially (drone on the ground)")
    await asyncio.sleep(PUBLISH_INTERVAL_SECONDS * 2)  # one or two messages
    step(f"{DRONE_A_ID} ARMING ...")
    drone_a.set_armed(True)
    # Wait long enough for state_tracker to observe the transition + a couple
    # of messages of telemetry.
    await asyncio.sleep(PUBLISH_INTERVAL_SECONDS * 2)
    success(f"{DRONE_A_ID} is FLYING — see the 'Arm-state transition: ARMED' log line above")
    watch("Dashboard: drone-alpha is now FLYING.  Live altitude / voltage / distance update each refresh.")
    prompt(f"{DRONE_A_ID} flying.", auto=auto)

    # ── Phase 4: drone-bravo arms ───────────────────────────────────────
    banner(f"PHASE 4  {DRONE_B_ID} arms — both drones now flying")
    drone_b = DemoDronePublisher(DRONE_B_ID, base_voltage=15.8, home_lat=39.78, home_lon=-86.17)
    drone_b.start()
    await asyncio.sleep(PUBLISH_INTERVAL_SECONDS)
    drone_b.set_armed(True)
    await asyncio.sleep(PUBLISH_INTERVAL_SECONDS * 2)
    success(f"{DRONE_B_ID} is FLYING — both drones publishing")
    watch("Dashboard: two FLYING rows in zone demo-zone-001.")
    prompt("Both drones in flight.", auto=auto)

    # ── Phase 5: drone-alpha lands and re-arms (multi-segment demo) ─────
    banner(f"PHASE 5  {DRONE_A_ID} lands, then re-arms (multi-segment FLIGHT_SEGMENT demo)")
    step(f"{DRONE_A_ID} DISARMING ...")
    drone_a.set_armed(False)
    await asyncio.sleep(PUBLISH_INTERVAL_SECONDS * 2)
    success(f"{DRONE_A_ID} now LANDED — segment 1 closed")
    await asyncio.sleep(PUBLISH_INTERVAL_SECONDS)
    step(f"{DRONE_A_ID} ARMING again (second flight)...")
    drone_a.set_armed(True)
    await asyncio.sleep(PUBLISH_INTERVAL_SECONDS * 2)
    success(f"{DRONE_A_ID} FLYING again — segment 2 open")
    watch("Dashboard: drone-alpha 'Segments' column shows '1 + 1 open'.  When SADE finalizes, the payload will carry TWO FLIGHT_SEGMENT events.")
    prompt(f"{DRONE_A_ID} now has one closed segment + one open.", auto=auto)

    # ── Phase 6: SADE sends exit-request for drone-bravo ────────────────
    banner(f"PHASE 6  SADE sends exit-request for {DRONE_B_ID}")
    step(f"{DRONE_B_ID} DISARMING and going silent (simulating end-of-flight)...")
    drone_b.set_armed(False)
    await asyncio.sleep(PUBLISH_INTERVAL_SECONDS)
    drone_b.stop()
    step("(publisher stopped — drone-bravo is now silent)")
    await send_exit_request(base_url, fid_b)
    step(f"Exit-grace task running.  Will finalize after {DEMO_GRACE_PERIOD_SECONDS:.0f}s of silence.")
    watch("Dashboard: drone-bravo flips to EXIT_REQUESTED for the duration of the grace period.")
    prompt("Watch the grace period elapse...", auto=auto, auto_seconds=DEMO_GRACE_PERIOD_SECONDS + 4.0)

    # The grace period should have elapsed by now and the catcher should
    # have received the finalize POST.  But under interactive mode the user
    # might have skipped past quickly, so wait a bit more if needed.
    deadline = time.monotonic() + 15.0
    while not any(r.get("flight_session_id") == fid_b for r in catcher.received):
        if time.monotonic() > deadline:
            fail(f"Grace period elapsed but catcher hasn't received {fid_b}'s finalize POST yet.")
            break
        await asyncio.sleep(0.5)
    else:
        success(f"{DRONE_B_ID} finalized end-to-end (catcher above) — registry should be down to 1 session")

    await show_health(base_url)
    watch("Dashboard: drone-bravo is GONE.  Only drone-alpha remains.")
    prompt(f"{DRONE_B_ID} fully closed out.", auto=auto)

    # ── Phase 7: same flow for drone-alpha ──────────────────────────────
    banner(f"PHASE 7  Same exit flow for {DRONE_A_ID}")
    step(f"{DRONE_A_ID} DISARMING and going silent ...")
    drone_a.set_armed(False)
    await asyncio.sleep(PUBLISH_INTERVAL_SECONDS)
    drone_a.stop()
    await send_exit_request(base_url, fid_a)
    step("Waiting for grace period to elapse...")
    deadline = time.monotonic() + DEMO_GRACE_PERIOD_SECONDS + 15.0
    while not any(r.get("flight_session_id") == fid_a for r in catcher.received):
        if time.monotonic() > deadline:
            fail(f"Catcher did not receive {fid_a}'s finalize POST in time.")
            break
        await asyncio.sleep(0.5)
    else:
        success(
            f"{DRONE_A_ID} finalized — note the TWO FLIGHT_SEGMENT events in the catcher output above"
        )

    await show_health(base_url)
    watch("Dashboard: empty — registry has zero active sessions.")
    prompt("All drones finalized.", auto=auto)

    # ── Phase 8: shutdown ───────────────────────────────────────────────
    banner("PHASE 8  Shutdown")
    step("Cancelling pipeline task ...")
    pipeline_task.cancel()
    await asyncio.gather(pipeline_task, return_exceptions=True)
    success("Pipeline shut down")

    step("Cancelling FastAPI server task ...")
    api_task.cancel()
    await asyncio.gather(api_task, return_exceptions=True)
    success("FastAPI shut down")

    step("Stopping SADE catcher ...")
    catcher.stop()
    success("Catcher shut down")

    print()
    success(f"Demo complete.  Catcher received {len(catcher.received)} finalization payload(s).")
    print()


# ── Entry point ──────────────────────────────────────────────────────────────


def configure_logging(verbose: bool) -> None:
    """Configure log output for the demo.

    INFO across the board EXCEPT the per-message worker spam, which fires
    once per telemetry message and quickly fills the screen.  --verbose
    promotes everything to DEBUG.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format=f"{DIM}%(asctime)s %(levelname)s %(name)s -{RESET} %(message)s",
        datefmt="%H:%M:%S",
    )
    if not verbose:
        # Suppress the ~once-per-message "worker updated drone_id=..." line —
        # at PUBLISH_INTERVAL_SECONDS=2 with two drones it's still 1 line/s
        # which makes the demo console hard to read.
        logging.getLogger("app.ingestion.workers").setLevel(logging.WARNING)
        # Pipeline metrics line is once per minute with our long interval —
        # but suppress it anyway, the demo doesn't need it.
        logging.getLogger("uvicorn").setLevel(logging.WARNING)
        logging.getLogger("uvicorn.error").setLevel(logging.WARNING)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Auto-advance through pauses with fixed delays (no Enter required)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Promote logging to DEBUG (shows every per-message worker log line)",
    )
    args = parser.parse_args()

    configure_logging(args.verbose)

    try:
        asyncio.run(run_demo(args))
    except KeyboardInterrupt:
        print()
        print(f"{YELLOW}Interrupted — cleaning up.{RESET}")
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
