#!/usr/bin/env python3
"""Main entry point for the asyncio telemetry pipeline.

This module wires together the transport, queue, worker, and local CSV output
pieces for the mission-tracking subsystem.

Two session modes are supported:
1. ``local``: telemetry can auto-create a synthetic active session for a drone.
   This keeps local testing simple and allows the MQTT simulator to run without
   any AWS approval workflow in front of it.
2. ``aws``: telemetry is only processed for drones that already have an active
   approved session in ``ActiveSessionRegistry``.

That separation lets the local test harness stay lightweight while still giving
us a production-like mode for future AWS integration.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os

from app.monitoring.active_session_registry import ActiveSessionRegistry
from app.monitoring.mission_row_builder import MissionRowBuilder
from app.common.mission_row_writer import MissionCsvWriter
from app.ingestion.mqtt_client import TelemetryMqttIngestionClient
from app.monitoring.memory_sampler import MemorySampler, memory_sampler_loop
from app.monitoring.pipeline_metrics import PipelineMetrics
from app.monitoring.state_tracker import DroneStateTracker
from app.ingestion.workers import telemetry_worker
from app.sending.tracker_finalizer import get_tracker_finalized_url

LOGGER = logging.getLogger(__name__)


async def pipeline_metrics_logger(
    metrics: PipelineMetrics,
    queue: asyncio.Queue[dict[str, object]],
    *,
    interval_seconds: float,
) -> None:
    """Log queue depth and latency summaries at a fixed interval.

    This gives local scale tests a lightweight way to observe ingestion health
    without adding any external monitoring stack.
    """
    while True:
        await asyncio.sleep(max(1.0, interval_seconds))
        snapshot = metrics.snapshot(queue.qsize())
        LOGGER.info(
            "Pipeline metrics: queue_current=%s queue_max=%s enqueued=%s processed=%s failed=%s dropped=%s final_rows=%s queue_latency_avg_ms=%.2f queue_latency_max_ms=%.2f processing_avg_ms=%.2f processing_max_ms=%.2f",
            snapshot.current_queue_depth,
            snapshot.max_queue_depth,
            snapshot.enqueued_messages,
            snapshot.processed_messages,
            snapshot.failed_messages,
            snapshot.dropped_messages,
            snapshot.final_rows_written,
            snapshot.average_queue_latency_ms,
            snapshot.max_queue_latency_ms,
            snapshot.average_processing_ms,
            snapshot.max_processing_ms,
        )


def build_arg_parser() -> argparse.ArgumentParser:
    """Build CLI parser for local/runtime configuration."""
    parser = argparse.ArgumentParser(description="Run local drone telemetry pipeline")
    # MQTT_BROKER_HOST is the container-friendly name; MQTT_BROKER kept for
    # backwards compatibility with existing local scripts and env files.
    parser.add_argument(
        "--broker",
        default=os.getenv("MQTT_BROKER_HOST") or os.getenv("MQTT_BROKER", "localhost"),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("MQTT_BROKER_PORT") or os.getenv("MQTT_PORT", "1883")),
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("MQTT_TOPIC", "status_message,update_drone"),
        help=(
            "MQTT telemetry topic(s) to subscribe to.  Accepts a single "
            "topic name or a comma-separated list (e.g. 'status_message,"
            "update_drone').  Defaults cover both topic names currently "
            "active in the fleet."
        ),
    )
    parser.add_argument(
        "--out",
        default=os.getenv("MISSION_ROWS_OUT", "mission_rows.csv"),
        help="Local CSV output path for mission summary rows",
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=int(os.getenv("QUEUE_SIZE", "10000")),
        help="Max number of messages buffered in memory",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.getenv("WORKER_COUNT", "1")),
        help="Number of async telemetry worker tasks",
    )
    parser.add_argument(
        "--shutdown-timeout",
        type=float,
        default=float(os.getenv("SHUTDOWN_TIMEOUT", "5")),
        help="Seconds to wait for queue drain during shutdown",
    )
    parser.add_argument(
        "--idle-warning-seconds",
        type=float,
        default=float(os.getenv("IDLE_WARNING_SECONDS", "300")),
        help="Log warning when no telemetry messages are received for this long",
    )
    parser.add_argument(
        "--session-source-mode",
        default=os.getenv("SESSION_SOURCE_MODE", "local"),
        choices=["local", "aws"],
        help=(
            "How telemetry resolves active sessions. "
            "'local' auto-creates synthetic sessions for testing; "
            "'aws' requires a pre-registered approved session."
        ),
    )
    parser.add_argument(
        "--metrics-log-interval",
        type=float,
        default=float(os.getenv("METRICS_LOG_INTERVAL", "30")),
        help="Seconds between queue-depth and latency metric log lines",
    )
    parser.add_argument(
        "--memory-sample-interval",
        type=float,
        default=float(os.getenv("MEMORY_SAMPLE_INTERVAL", "0")),
        help=(
            "Seconds between RSS memory samples. "
            "0 disables memory sampling. "
            "The final shutdown metrics line includes peak/average/current RSS when enabled."
        ),
    )
    parser.add_argument(
        "--finalize-to-api",
        action="store_true",
        default=os.getenv("FINALIZE_TO_API", "").lower() in {"1", "true", "yes"},
        help=(
            "POST finalized telemetry to the SADE tracker-session-finalized endpoint "
            "when each drone mission completes. "
            "CSV writing is still active when --out is set, so both can run together. "
            "Use this for non-local deployments against the SADE AWS API. "
            "Also enabled by setting FINALIZE_TO_API=true in the environment."
        ),
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )

    # ── MQTT auth / TLS ──────────────────────────────────────────────────────
    # Required when connecting to a secured broker (cloud MQTT, AWS IoT Core).
    # Leave unset for local/unauthenticated brokers.
    parser.add_argument(
        "--mqtt-username",
        default=os.getenv("MQTT_USERNAME", ""),
        help="MQTT broker username (MQTT_USERNAME env var)",
    )
    parser.add_argument(
        "--mqtt-password",
        default=os.getenv("MQTT_PASSWORD", ""),
        help="MQTT broker password (MQTT_PASSWORD env var)",
    )
    parser.add_argument(
        "--mqtt-tls",
        action="store_true",
        default=os.getenv("MQTT_TLS_ENABLED", "").lower() in {"1", "true", "yes"},
        help="Enable TLS on the MQTT connection (MQTT_TLS_ENABLED env var)",
    )

    # ── mTLS (AWS IoT Core) ──────────────────────────────────────────────────
    # All three paths must be set together.  Leave unset when not using mTLS.
    # See TelemetryMqttIngestionClient for the full mode-selection rules.
    parser.add_argument(
        "--mqtt-ca-cert",
        default=os.getenv("MQTT_CA_CERT_PATH", ""),
        help=(
            "Path to the CA certificate used to verify the MQTT broker "
            "(MQTT_CA_CERT_PATH env var). Required for AWS IoT Core mTLS."
        ),
    )
    parser.add_argument(
        "--mqtt-client-cert",
        default=os.getenv("MQTT_CLIENT_CERT_PATH", ""),
        help=(
            "Path to this client's signed X.509 certificate "
            "(MQTT_CLIENT_CERT_PATH env var). Required for AWS IoT Core mTLS."
        ),
    )
    parser.add_argument(
        "--mqtt-private-key",
        default=os.getenv("MQTT_PRIVATE_KEY_PATH", ""),
        help=(
            "Path to this client's private key matching the client certificate "
            "(MQTT_PRIVATE_KEY_PATH env var). Required for AWS IoT Core mTLS. "
            "Must be a bind-mounted path — never bake key material into the image."
        ),
    )
    parser.add_argument(
        "--mqtt-client-id",
        default=os.getenv("MQTT_CLIENT_ID", ""),
        help=(
            "MQTT client identifier (MQTT_CLIENT_ID env var). "
            "Required for AWS IoT Core: the IoT policy attached to your "
            "certificate typically restricts which client IDs the cert may "
            "use, so paho's random default will be silently rejected. "
            "Leave unset for local / unauthenticated brokers."
        ),
    )

    # ── FastAPI server (used by run_service / container mode) ────────────────
    parser.add_argument(
        "--api-host",
        default=os.getenv("API_HOST", "0.0.0.0"),
        help="Host for the FastAPI webhook server to bind on (API_HOST env var)",
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=int(os.getenv("API_PORT", "8000")),
        help="Port for the FastAPI webhook server (API_PORT env var)",
    )

    return parser


async def idle_message_watchdog(
    mqtt_client: TelemetryMqttIngestionClient,
    *,
    idle_warning_seconds: float,
    check_interval_seconds: float = 30.0,
) -> None:
    """Log a warning when no inbound MQTT messages are seen for a threshold."""
    loop = asyncio.get_running_loop()
    started_at = loop.time()
    last_alert_bucket = -1

    while True:
        await asyncio.sleep(max(1.0, check_interval_seconds))

        idle_since_last = mqtt_client.seconds_since_last_message()
        idle_duration = idle_since_last if idle_since_last is not None else (loop.time() - started_at)

        if idle_duration < idle_warning_seconds:
            last_alert_bucket = -1
            continue

        current_bucket = int(idle_duration // idle_warning_seconds)
        if current_bucket == last_alert_bucket:
            continue

        last_alert_bucket = current_bucket
        LOGGER.warning(
            "No MQTT telemetry messages received for %.1f minutes.",
            idle_duration / 60.0,
        )


def _parse_topics(value: str) -> list[str]:
    """Parse a comma-separated MQTT topic list, dropping blanks/whitespace."""
    return [t.strip() for t in (value or "").split(",") if t.strip()]


async def run_pipeline(args: argparse.Namespace) -> None:
    """Create and run the telemetry pipeline until interrupted."""
    # Fail-fast on misconfigured finalization: if the pipeline is going to POST
    # to SADE, we refuse to start without a configured TRACKER_FINALIZED_URL.
    # Silently POSTing to the wrong backend is worse than refusing to start.
    finalize_to_api = getattr(args, "finalize_to_api", False)
    resolved_tracker_url: str | None = None
    if finalize_to_api:
        resolved_tracker_url = get_tracker_finalized_url()

    queue: asyncio.Queue[dict[str, object]] = asyncio.Queue(maxsize=args.queue_size)
    state_tracker = getattr(args, "state_tracker", None) or DroneStateTracker()
    row_writer = MissionCsvWriter(out_path=args.out)
    row_builder = MissionRowBuilder()
    metrics = PipelineMetrics()
    memory_sampler = MemorySampler()

    # Future AWS integration can inject a pre-populated registry onto args.
    session_registry = getattr(args, "session_registry", None) or ActiveSessionRegistry()

    topics = _parse_topics(args.topic)
    if not topics:
        raise RuntimeError(
            "MQTT_TOPIC is empty.  Set at least one topic name "
            "(comma-separated for multiple)."
        )

    mqtt_client = TelemetryMqttIngestionClient(
        queue,
        broker=args.broker,
        port=args.port,
        topics=topics,
        metrics=metrics,
        # Auth / TLS fields are optional; local runs leave these as empty/False.
        username=getattr(args, "mqtt_username", "") or None,
        password=getattr(args, "mqtt_password", "") or None,
        tls_enabled=getattr(args, "mqtt_tls", False),
        # mTLS — all three paths must be set together for AWS IoT Core.
        # Empty strings (local / non-mTLS deployments) are normalised to None
        # so the client's mode selection sees "unset" rather than a bogus path.
        ca_cert_path=getattr(args, "mqtt_ca_cert", "") or None,
        client_cert_path=getattr(args, "mqtt_client_cert", "") or None,
        private_key_path=getattr(args, "mqtt_private_key", "") or None,
        client_id=getattr(args, "mqtt_client_id", "") or None,
    )
    mqtt_client.start()

    worker_tasks = [
        asyncio.create_task(
            telemetry_worker(
                queue,
                state_tracker,
                worker_name=f"telemetry-worker-{index + 1}",
                row_writer=row_writer,
                row_builder=row_builder,
                session_registry=session_registry,
                session_source_mode=args.session_source_mode,
                metrics=metrics,
            )
        )
        for index in range(max(1, args.workers))
    ]

    metrics_task = asyncio.create_task(
        pipeline_metrics_logger(
            metrics,
            queue,
            interval_seconds=max(1.0, args.metrics_log_interval),
        )
    )

    watchdog_task = asyncio.create_task(
        idle_message_watchdog(
            mqtt_client,
            idle_warning_seconds=max(1.0, args.idle_warning_seconds),
            check_interval_seconds=min(30.0, max(1.0, args.idle_warning_seconds / 5.0)),
        )
    )

    memory_sample_interval = float(getattr(args, "memory_sample_interval", 0.0) or 0.0)
    memory_task = asyncio.create_task(
        memory_sampler_loop(memory_sampler, interval_seconds=memory_sample_interval)
    )

    LOGGER.info(
        "Pipeline started: broker=%s port=%s topics=%s workers=%s out=%s session_source_mode=%s idle_warning=%ss metrics_log_interval=%ss finalize_to_api=%s tracker_url=%s",
        args.broker,
        args.port,
        ",".join(topics),
        len(worker_tasks),
        args.out,
        args.session_source_mode,
        max(1.0, args.idle_warning_seconds),
        max(1.0, args.metrics_log_interval),
        finalize_to_api,
        resolved_tracker_url or "(not configured — finalization disabled)",
    )

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        LOGGER.info("Pipeline cancellation received")
        raise
    finally:
        LOGGER.info("Shutting down pipeline")

        metrics_task.cancel()
        watchdog_task.cancel()
        memory_task.cancel()
        await asyncio.gather(metrics_task, watchdog_task, memory_task, return_exceptions=True)

        mqtt_client.stop()

        if queue.qsize() > 0:
            LOGGER.info("Waiting for queue drain: pending=%s", queue.qsize())
        try:
            await asyncio.wait_for(queue.join(), timeout=max(0.1, args.shutdown_timeout))
        except asyncio.TimeoutError:
            LOGGER.warning("Queue did not fully drain before shutdown timeout")

        for task in worker_tasks:
            task.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)

        snapshot = metrics.snapshot(queue.qsize())
        memory_snapshot = memory_sampler.snapshot()
        LOGGER.info(
            "Shutdown metrics: queue_current=%s queue_max=%s enqueued=%s processed=%s failed=%s dropped=%s final_rows=%s queue_latency_avg_ms=%.2f queue_latency_max_ms=%.2f processing_avg_ms=%.2f processing_max_ms=%.2f memory_samples=%s rss_current_mb=%.2f rss_peak_mb=%.2f rss_avg_mb=%.2f",
            snapshot.current_queue_depth,
            snapshot.max_queue_depth,
            snapshot.enqueued_messages,
            snapshot.processed_messages,
            snapshot.failed_messages,
            snapshot.dropped_messages,
            snapshot.final_rows_written,
            snapshot.average_queue_latency_ms,
            snapshot.max_queue_latency_ms,
            snapshot.average_processing_ms,
            snapshot.max_processing_ms,
            memory_snapshot.sample_count,
            memory_snapshot.rss_current_mb,
            memory_snapshot.rss_peak_mb,
            memory_snapshot.rss_average_mb,
        )
        LOGGER.info(
            "Shutdown complete. Active unfinished mission states=%s active sessions=%s",
            state_tracker.count(),
            session_registry.count(),
        )


async def run_service(args: argparse.Namespace) -> None:
    """Run the FastAPI webhook server and the MQTT pipeline together.

    Both components share the same ``ActiveSessionRegistry`` and
    ``DroneStateTracker`` instances so that sessions registered or exited
    via the API are immediately visible to the telemetry workers (and vice
    versa) without any IPC.

    This is the production / container startup path.  Local testing uses
    ``run_pipeline()`` directly and manages uvicorn separately.
    """
    import uvicorn  # imported here to keep the pipeline path dependency-free

    from app.api.server import app as fastapi_app
    from app.api.server import registry as api_registry
    from app.api.server import state_tracker as api_state_tracker

    # Inject the API module's shared instances into the pipeline args so both
    # the API server and the MQTT workers share the same in-memory state.
    args.session_registry = api_registry
    args.state_tracker = api_state_tracker

    # Force aws session mode when the approval API is running — the API is the
    # gate; the pipeline should only accept pre-registered sessions.
    args.session_source_mode = "aws"

    LOGGER.info(
        "Starting service: api=%s:%s broker=%s:%s finalize_to_api=%s",
        args.api_host,
        args.api_port,
        args.broker,
        args.port,
        args.finalize_to_api,
    )

    # Run the MQTT pipeline as a background asyncio task.
    pipeline_task = asyncio.create_task(run_pipeline(args), name="mqtt-pipeline")

    # Uvicorn serves the FastAPI app in the foreground.  When it exits (Ctrl-C
    # or SIGTERM), we cancel the pipeline task so everything shuts down cleanly.
    config = uvicorn.Config(
        fastapi_app,
        host=args.api_host,
        port=args.api_port,
        log_level=args.log_level.lower(),
    )
    server = uvicorn.Server(config)

    try:
        await server.serve()
    finally:
        pipeline_task.cancel()
        await asyncio.gather(pipeline_task, return_exceptions=True)


def main() -> int:
    """Parse args, configure logging, and run the full service."""
    parser = build_arg_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    try:
        asyncio.run(run_service(args))
    except KeyboardInterrupt:
        LOGGER.info("Interrupted by user")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
