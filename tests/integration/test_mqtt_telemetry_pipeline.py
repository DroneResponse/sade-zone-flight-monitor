#!/usr/bin/env python3
"""Run a full local MQTT telemetry test against a local Mosquitto broker.

This script tests the **real MQTT telemetry monitoring path**: fake drones
publish live telemetry over MQTT, the pipeline ingests and tracks it, and
mission rows are written when each drone finishes.

For testing the **test_overrides HTTP stub path** (no MQTT, no broker, no
drones), use ``tests/integration/test_stub_finalization_override.py`` instead.

This script keeps the local setup practical:
- starts a local Mosquitto broker on localhost:1883 when needed
- starts a FastAPI session registration server (by default)
- starts the existing ingestion pipeline subscribed to the status_message and
  update_drone topics (the publisher uses the first one)
- sends an approved entry POST for each fake drone before it begins publishing
- starts multiple fake drone telemetry publishers
- writes ingestion results to a CSV file
- shuts everything down cleanly on timeout or Ctrl+C

Default flow (approval API enabled):
  1. Mosquitto broker starts
  2. FastAPI webhook server starts on localhost:8000
  3. Ingestion pipeline starts in 'aws' session mode (only pre-registered drones)
  4. Each fake drone POSTs a session registration to /flight-monitor/register-session
  5. Only after approval succeeds does the drone begin MQTT telemetry publishing
  6. On mission complete the pipeline writes one CSV row per drone

Pass --skip-approval-api to revert to the original 'local' session mode where
the pipeline auto-creates synthetic sessions without any HTTP approval step.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import logging
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
APP_DIR = REPO_ROOT / "app"
LOCAL_TEST_DIR = REPO_ROOT / "local_testing"
OUTPUT_DIR = REPO_ROOT / "local_test_output"

LOGGER = logging.getLogger("run_local_test")


# ── Module loading helpers ────────────────────────────────────────────────────

def _load_module(module_name: str, file_path: Path) -> ModuleType:
    """Load a Python module directly from a repository file path."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module {module_name} from {file_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _ensure_import_paths() -> None:
    """Add local module directories so existing imports resolve unchanged."""
    for path in (REPO_ROOT, LOCAL_TEST_DIR):
        path_text = str(path)
        if path_text not in sys.path:
            sys.path.insert(0, path_text)


# ── Mosquitto broker management ───────────────────────────────────────────────

def _is_port_open(host: str, port: int) -> bool:
    """Return True when something is already listening on the given TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex((host, port)) == 0


class MosquittoProcess:
    """Manage a local Mosquitto process, while reusing an existing broker if present."""

    def __init__(self, host: str, port: int, log_path: Path) -> None:
        self.host = host
        self.port = port
        self.log_path = log_path
        self.process: subprocess.Popen | None = None
        self.started_here = False
        self._log_handle = None

    def start(self) -> None:
        """Start Mosquitto only if localhost:1883 is not already in use."""
        if _is_port_open(self.host, self.port):
            LOGGER.info("Reusing existing Mosquitto broker at %s:%s", self.host, self.port)
            return

        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_handle = self.log_path.open("a", encoding="utf-8")
        self.process = subprocess.Popen(
            ["mosquitto", "-p", str(self.port), "-v"],
            stdout=self._log_handle,
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
        )
        self.started_here = True

        for _ in range(30):
            if _is_port_open(self.host, self.port):
                LOGGER.info("Started local Mosquitto broker at %s:%s", self.host, self.port)
                return
            if self.process.poll() is not None:
                raise RuntimeError("Mosquitto exited before opening the MQTT port")
            time.sleep(0.1)

        raise RuntimeError("Timed out waiting for Mosquitto to start listening")

    def stop(self) -> None:
        """Stop Mosquitto only when this script launched it."""
        if self.process is not None and self.started_here:
            LOGGER.info("Stopping local Mosquitto broker")
            self.process.terminate()
            with suppress(subprocess.TimeoutExpired):
                self.process.wait(timeout=5)
            if self.process.poll() is None:
                self.process.kill()
                self.process.wait(timeout=5)

        if self._log_handle is not None:
            self._log_handle.close()


# ── FastAPI webhook server helpers ────────────────────────────────────────────

async def _run_api_server(api_app, host: str, port: int) -> None:
    """Run the FastAPI webhook server as a co-operative asyncio task.

    Uvicorn is started programmatically so it shares the running event loop
    with the ingestion pipeline and publisher schedule. This lets all three
    components share in-memory state (specifically the ActiveSessionRegistry)
    without any IPC.
    """
    import uvicorn

    config = uvicorn.Config(
        api_app,
        host=host,
        port=port,
        # Suppress uvicorn's per-request access logs during test runs so the
        # test output stays readable. Set to "info" when debugging the API.
        log_level="warning",
    )
    server = uvicorn.Server(config)
    await server.serve()


async def _wait_for_api_ready(base_url: str, timeout: float = 15.0) -> None:
    """Poll /health until the API server is accepting connections.

    Called once after the API server task is started so that the first drone
    approval POST does not race against the server coming up.
    """
    health_url = f"{base_url}/health"
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while loop.time() < deadline:
        try:
            # urllib.request is blocking; run it in a thread so the event loop
            # stays responsive while we wait for the server to start.
            def _check() -> bool:
                resp = urllib.request.urlopen(health_url, timeout=1)
                return resp.status == 200

            ready = await asyncio.to_thread(_check)
            if ready:
                LOGGER.info("FastAPI webhook server is ready at %s", base_url)
                return
        except Exception:
            pass

        await asyncio.sleep(0.25)

    raise RuntimeError(
        f"FastAPI webhook server did not become ready within {timeout}s at {base_url}"
    )


async def _post_register_session(
    drone_id: str,
    api_base_url: str,
    *,
    runtime_seconds: float,
) -> bool:
    """POST a session registration to the local Flight Monitor endpoint.

    Each fake drone calls this once before beginning MQTT telemetry publishing.
    The API server registers the session in the shared ActiveSessionRegistry so
    the ingestion pipeline (running in 'aws' mode) will accept that drone's
    telemetry messages.

    Returns True when the server responds with action='registered', False on
    any failure (HTTP error, network error, or unexpected response body).
    """
    now = datetime.now(timezone.utc)
    payload = {
        "flight_session_id": f"local-flight-{drone_id}-{uuid4()}",
        "drone_id": drone_id,
        "pilot_id": f"local-pilot-{drone_id}",
        "requested_entry_time": now.isoformat(),
        "requested_exit_time": (now + timedelta(seconds=runtime_seconds + 60)).isoformat(),
        "requested_operation": {"operation_type": "LOCAL_TEST", "priority": "NORMAL"},
        "submitted_at": now.isoformat(),
    }

    url = f"{api_base_url}/flight-monitor/register-session"
    data = json.dumps(payload).encode()
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    LOGGER.info("[%s] Sending session registration POST → %s", drone_id, url)

    try:
        def _do_post() -> dict:
            with urllib.request.urlopen(request, timeout=10) as resp:
                return json.loads(resp.read())

        result = await asyncio.to_thread(_do_post)
        action = result.get("action")

        if action == "registered":
            LOGGER.info(
                "[%s] Session registered. flight_session_id=%s",
                drone_id,
                result.get("flight_session_id"),
            )
            return True

        LOGGER.warning(
            "[%s] Session registration not accepted. action=%s reason=%s",
            drone_id,
            action,
            result.get("reason"),
        )
        return False

    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        LOGGER.error(
            "[%s] Session registration HTTP error: %s — %s",
            drone_id, exc.code, body,
        )
        return False

    except Exception as exc:
        LOGGER.error(
            "[%s] Session registration request failed: %s",
            drone_id, exc,
        )
        return False


# ── Ingestion pipeline ────────────────────────────────────────────────────────

async def _run_ingestion_pipeline(
    args: argparse.Namespace,
    *,
    session_registry=None,
    session_source_mode: str | None = None,
) -> None:
    """Run the existing ingestion pipeline as an in-process async task.

    ``session_registry`` — when provided (approval API mode) this is the same
    ActiveSessionRegistry instance the FastAPI server writes to, so the pipeline
    sees approved sessions the moment they are registered via HTTP.

    ``session_source_mode`` — overrides args.session_source_mode when set.
    Approval API mode always forces 'aws' here so the pipeline only accepts
    drones that were pre-registered through the webhook.
    """
    ingestion_main = _load_module("local_ingestion_main", APP_DIR / "main.py")
    pipeline_args = SimpleNamespace(
        broker=args.broker,
        port=args.port,
        topic=args.topic,
        out=str(args.output_csv),
        queue_size=args.queue_size,
        workers=args.workers,
        shutdown_timeout=args.shutdown_timeout,
        idle_warning_seconds=args.idle_warning_seconds,
        session_source_mode=session_source_mode or args.session_source_mode,
        metrics_log_interval=args.metrics_log_interval,
        memory_sample_interval=getattr(args, "memory_sample_interval", 0.0),
        log_level=args.log_level,
        # Inject the shared registry so the pipeline and API server see the
        # same sessions. None here tells the pipeline to create its own.
        session_registry=session_registry,
        # Forward the finalize-to-api flag so workers POST to SADE on completion.
        finalize_to_api=getattr(args, "finalize_to_api", False),
    )
    await ingestion_main.run_pipeline(pipeline_args)


# ── Drone publisher helpers ───────────────────────────────────────────────────

def _build_publishers(args: argparse.Namespace) -> list:
    """Create a small fleet of autonomous telemetry publishers."""
    drone_sim = _load_module("local_drone_sim", LOCAL_TEST_DIR / "drone_sim.py")
    profiles = drone_sim.build_default_profiles(args.drone_count)
    return [
        drone_sim.SimulatedDronePublisher(
            profile,
            broker=args.broker,
            port=args.port,
            topic=args.topic,
            publish_interval=args.publish_interval,
            logger=LOGGER.info,
        )
        for profile in profiles
    ]


async def _run_publisher_schedule(
    args: argparse.Namespace,
    publishers: list,
    *,
    use_approval_api: bool = False,
    api_base_url: str = "",
) -> None:
    """Start publishers in staggered groups and stop each after its runtime.

    When ``use_approval_api`` is True each drone first sends an APPROVED entry
    POST to the FastAPI server. Telemetry publishing only begins after that
    POST succeeds. If approval fails the drone is skipped entirely so the CSV
    will only contain rows for drones the pipeline actually accepted.
    """
    if args.group_size <= 0:
        raise ValueError("group_size must be greater than zero")
    if args.group_stagger_seconds < 0:
        raise ValueError("group_stagger_seconds cannot be negative")
    if args.publisher_runtime_seconds <= 0:
        raise ValueError("publisher_runtime_seconds must be greater than zero")

    async def run_publisher_lifecycle(publisher, start_delay_seconds: float) -> None:
        # Stagger group startup so we don't slam the broker all at once.
        await asyncio.sleep(start_delay_seconds)

        # ── Step 1: Register session via the Flight Monitor endpoint ──────────
        if use_approval_api:
            approved = await _post_register_session(
                publisher.profile.drone_id,
                api_base_url,
                runtime_seconds=args.publisher_runtime_seconds,
            )
            if not approved:
                LOGGER.error(
                    "[%s] Session registration failed — skipping MQTT telemetry publishing",
                    publisher.profile.drone_id,
                )
                return

        # ── Step 2: Begin MQTT telemetry publishing ──────────────────────────
        LOGGER.info("[%s] Starting MQTT telemetry publishing", publisher.profile.drone_id)
        publisher.start()
        await asyncio.sleep(args.publisher_runtime_seconds)
        publisher.stop()

    lifecycle_tasks = []
    for index, publisher in enumerate(publishers):
        group_index = index // args.group_size
        start_delay_seconds = float(group_index * args.group_stagger_seconds)
        lifecycle_tasks.append(
            asyncio.create_task(run_publisher_lifecycle(publisher, start_delay_seconds))
        )

    total_groups = ((len(publishers) - 1) // args.group_size) + 1 if publishers else 0
    LOGGER.info(
        "Publisher schedule started: drones=%s groups=%s group_size=%s stagger=%ss runtime_per_drone=%ss approval_api=%s",
        len(publishers),
        total_groups,
        args.group_size,
        args.group_stagger_seconds,
        args.publisher_runtime_seconds,
        use_approval_api,
    )
    await asyncio.gather(*lifecycle_tasks)

    # ── Publisher totals ─────────────────────────────────────────────────────
    # After all drone lifecycles finish, each publisher's ``published_count``
    # is the number of in-mission telemetry messages it sent (not counting the
    # final ``mission_completed`` message emitted during stop()).  Add one per
    # drone so the stress-test driver can compare total-sent against the
    # pipeline's ``enqueued`` count to estimate broker-side drops.
    per_drone_sent = [publisher.published_count + 1 for publisher in publishers]
    total_published = sum(per_drone_sent)
    LOGGER.info(
        "Publisher totals: published=%s drones=%s avg_per_drone=%.1f",
        total_published,
        len(publishers),
        (total_published / len(publishers)) if publishers else 0.0,
    )


# ── Main test coordinator ─────────────────────────────────────────────────────

async def run_local_test(args: argparse.Namespace) -> None:
    """Coordinate broker, API server, ingestion pipeline, drone publishers, and shutdown.

    Startup order when the approval API is enabled:
      1. Mosquitto broker
      2. FastAPI webhook server  ← shares registry with pipeline
      3. Ingestion pipeline      ← 'aws' mode, shared registry
      4. Per-drone approval POST ← registers session before telemetry starts
      5. MQTT telemetry publish  ← pipeline accepts because session is registered

    Startup order when the approval API is disabled (--skip-approval-api):
      1. Mosquitto broker
      2. Ingestion pipeline  ← 'local' mode, auto-creates sessions
      3. MQTT telemetry publish
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    broker = MosquittoProcess(args.broker, args.port, args.mosquitto_log)
    broker.start()

    use_approval_api = not args.skip_approval_api
    api_base_url = f"http://{args.api_host}:{args.api_port}"

    publishers = []
    pipeline_task = None
    api_task = None
    schedule_task = None
    stop_event = asyncio.Event()
    shared_registry = None
    effective_session_mode = args.session_source_mode

    def request_stop() -> None:
        LOGGER.info("Shutdown requested")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, request_stop)

    try:
        # ── Start FastAPI webhook server (approval API mode only) ────────────
        if use_approval_api:
            # Load api/server.py via _load_module so the import resolves at
            # runtime using the sys.path set by _ensure_import_paths(), matching
            # the pattern used for all other pipeline modules in this file.
            api_server_mod = _load_module(
                "app.api.server", APP_DIR / "api" / "server.py"
            )
            fastapi_app = api_server_mod.app

            # The pipeline will receive this same registry instance so that
            # sessions registered by the API are immediately visible to workers.
            shared_registry = api_server_mod.registry

            LOGGER.info(
                "Starting FastAPI webhook server at %s (api_host=%s api_port=%s)",
                api_base_url,
                args.api_host,
                args.api_port,
            )
            api_task = asyncio.create_task(
                _run_api_server(fastapi_app, args.api_host, args.api_port),
                name="api-server",
            )

            # Block until the server is actually accepting connections before
            # the pipeline or drones try to use it.
            await _wait_for_api_ready(api_base_url)

            # Force 'aws' mode: the pipeline must only track sessions that were
            # explicitly registered through the approval endpoint.
            effective_session_mode = "aws"
            LOGGER.info(
                "Approval API enabled — pipeline running in 'aws' session mode. "
                "Drones must POST to /flight-monitor/register-session before telemetry is accepted."
            )
        else:
            LOGGER.info(
                "Approval API disabled (--skip-approval-api). "
                "Pipeline running in '%s' session mode.",
                effective_session_mode,
            )

        # ── Start ingestion pipeline ─────────────────────────────────────────
        pipeline_task = asyncio.create_task(
            _run_ingestion_pipeline(
                args,
                session_registry=shared_registry,
                session_source_mode=effective_session_mode,
            ),
            name="ingestion-pipeline",
        )
        # Give the pipeline a moment to connect to the broker and start its
        # worker tasks before any drone telemetry arrives.
        await asyncio.sleep(args.ingestion_start_delay)

        # ── Build drone publishers and start approval + publish schedule ──────
        publishers = _build_publishers(args)
        schedule_task = asyncio.create_task(
            _run_publisher_schedule(
                args,
                publishers,
                use_approval_api=use_approval_api,
                api_base_url=api_base_url,
            ),
            name="publisher-schedule",
        )

        LOGGER.info(
            "Local MQTT test running: drones=%s output_csv=%s session_mode=%s approval_api=%s",
            len(publishers),
            args.output_csv,
            effective_session_mode,
            use_approval_api,
        )

        # Wait for either the publisher schedule to finish or a shutdown signal.
        wait_tasks = [
            schedule_task,
            asyncio.create_task(stop_event.wait(), name="shutdown-wait"),
        ]
        done, pending = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        if schedule_task in done and not stop_event.is_set():
            LOGGER.info("Publisher schedule completed")
        elif stop_event.is_set():
            LOGGER.info("Stopping local test early due to shutdown request")

        if schedule_task is not None and not schedule_task.done():
            schedule_task.cancel()
            await asyncio.gather(schedule_task, return_exceptions=True)

    finally:
        # Stop any publishers that are still running (e.g. on early shutdown).
        for publisher in publishers:
            with suppress(Exception):
                publisher.stop()

        if pipeline_task is not None:
            pipeline_task.cancel()
            await asyncio.gather(pipeline_task, return_exceptions=True)

        # Shut down the API server task if we started one.
        if api_task is not None:
            api_task.cancel()
            await asyncio.gather(api_task, return_exceptions=True)
            LOGGER.info("FastAPI webhook server stopped")

        broker.stop()


# ── CLI ───────────────────────────────────────────────────────────────────────

def build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI for a repeatable local end-to-end test run."""
    parser = argparse.ArgumentParser(description="Run a full local MQTT drone ingestion test")

    # MQTT / broker
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--topic", default="status_message", help="Telemetry topic to publish/subscribe")

    # Drone simulation
    parser.add_argument("--drone-count", type=int, default=3, help="Number of fake drones to publish")
    parser.add_argument("--publish-interval", type=float, default=0.1, help="Seconds between telemetry messages")
    parser.add_argument(
        "--publisher-runtime-seconds",
        type=float,
        default=20.0,
        help="How long each simulated drone runs before sending mission-complete and stopping",
    )
    parser.add_argument("--group-size", type=int, default=3, help="How many drones to start in each launch group")
    parser.add_argument(
        "--group-stagger-seconds",
        type=float,
        default=0.0,
        help="Seconds to wait before starting the next group of drones",
    )

    # Pipeline tuning
    parser.add_argument("--workers", type=int, default=1, help="Number of ingestion worker tasks")
    parser.add_argument("--queue-size", type=int, default=10000, help="Telemetry queue size")
    parser.add_argument("--shutdown-timeout", type=float, default=5.0, help="Queue drain timeout during shutdown")
    parser.add_argument("--idle-warning-seconds", type=float, default=60.0, help="Ingestion idle warning threshold")
    parser.add_argument(
        "--metrics-log-interval",
        type=float,
        default=15.0,
        help="Seconds between queue-depth and latency metric log lines",
    )
    parser.add_argument(
        "--memory-sample-interval",
        type=float,
        default=0.0,
        help=(
            "Seconds between RSS memory samples inside the pipeline process. "
            "0 disables sampling. The final shutdown metrics line adds peak/avg/current RSS when enabled."
        ),
    )
    parser.add_argument(
        "--ingestion-start-delay",
        type=float,
        default=1.0,
        help="Seconds to wait for the pipeline to be ready before drones start publishing",
    )
    parser.add_argument(
        "--session-source-mode",
        default="local",
        choices=["local", "aws"],
        help=(
            "How telemetry resolves active sessions. Ignored when --skip-approval-api is not set "
            "because the approval API always forces 'aws' mode."
        ),
    )

    # FastAPI webhook server
    parser.add_argument(
        "--api-host",
        default="127.0.0.1",
        help="Host for the FastAPI flight-monitor webhook server",
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=8000,
        help="Port for the FastAPI flight-monitor webhook server",
    )
    parser.add_argument(
        "--skip-approval-api",
        action="store_true",
        default=False,
        help=(
            "Disable the FastAPI flight-monitor registration flow and revert to the original "
            "'local' session mode where the pipeline auto-creates sessions. "
            "Use this when you want the old behaviour without the HTTP layer."
        ),
    )

    parser.add_argument(
        "--finalize-to-api",
        action="store_true",
        default=False,
        help=(
            "POST finalized telemetry to the SADE tracker-session-finalized endpoint "
            "when each drone mission completes. "
            "Can be combined with --output-csv to write CSV and POST simultaneously."
        ),
    )

    # Output paths
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=OUTPUT_DIR / "mission_rows.csv",
        help="CSV file written by the ingestion pipeline",
    )
    parser.add_argument(
        "--mosquitto-log",
        type=Path,
        default=OUTPUT_DIR / "mosquitto.log",
        help="Broker log file used when this script starts Mosquitto",
    )
    parser.add_argument(
        "--runner-log",
        type=Path,
        default=OUTPUT_DIR / "run_local_test.log",
        help="Runner log file",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser


def configure_logging(args: argparse.Namespace) -> None:
    """Write runner logs to both stdout and a local log file for test review."""
    args.runner_log.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(args.runner_log, mode="a", encoding="utf-8"),
        ],
    )


def main() -> int:
    """CLI entry point for the local end-to-end MQTT test runner."""
    _ensure_import_paths()
    args = build_arg_parser().parse_args()
    configure_logging(args)

    try:
        asyncio.run(run_local_test(args))
    except KeyboardInterrupt:
        LOGGER.info("Interrupted by user")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
