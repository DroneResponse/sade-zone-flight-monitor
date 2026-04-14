"""Async queue consumer workers for telemetry processing.

This worker supports two session-resolution modes:
1. Local mode: automatically create a synthetic active session for any drone
   that starts publishing telemetry.
2. AWS mode: only process telemetry for drones that already have an approved
   active session in ``ActiveSessionRegistry``.

Once a session is resolved, the worker updates session-scoped mission state and
writes one final mission row when a mission-finish message is observed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from app.monitoring.active_session_registry import ActiveSessionRegistry
from app.monitoring.mission_row_builder import MissionRowBuilder
from app.common.mission_row_writer import MissionCsvWriter
from app.monitoring.pipeline_metrics import PipelineMetrics
from app.monitoring.state_tracker import DroneStateTracker
from app.sending.tracker_finalizer import build_finalization_payload, post_tracker_session_finalized

LOGGER = logging.getLogger(__name__)

TERMINAL_MISSION_STATUSES = {
    "complete",
    "completed",
    "done",
    "finished",
    "mission_complete",
    "mission_completed",
    "mission_finished",
}


def _extract_drone_id(payload: dict[str, Any]) -> str | None:
    """Extract drone_id from known field variants."""
    return (
        payload.get("drone_id")
        or payload.get("droneId")
        or payload.get("uavid")
        or payload.get("uavID")
        or payload.get("uav_id")
    )


def _extract_position(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Extract normalized position fields when available."""
    status = payload.get("status") if isinstance(payload.get("status"), dict) else {}
    location = status.get("location") if isinstance(status.get("location"), dict) else {}

    lat = location.get("latitude", location.get("lat"))
    lon = location.get("longitude", location.get("lon", location.get("lng")))
    alt = location.get("altitude", location.get("alt"))

    if lat is None and lon is None and alt is None:
        return None

    return {
        "latitude": lat,
        "longitude": lon,
        "altitude": alt,
        "raw": location,
    }


def _extract_mission_status(payload: dict[str, Any]) -> Any:
    """Extract best-effort mission/status field from payload."""
    status = payload.get("status") if isinstance(payload.get("status"), dict) else {}
    return payload.get("mission_status", status.get("status"))


def _extract_mode(payload: dict[str, Any]) -> Any:
    """Extract best-effort mode field from payload."""
    status = payload.get("status") if isinstance(payload.get("status"), dict) else {}
    return payload.get("mode", status.get("mode"))


def _is_terminal_mission_status(mission_status: Any) -> bool:
    """Return True when a mission status means the mission has finished."""
    if mission_status is None:
        return False
    return str(mission_status).strip().lower() in TERMINAL_MISSION_STATUSES


def parse_queue_message(message: dict[str, Any]) -> dict[str, Any]:
    """Parse one queue message from MQTT ingestion into normalized fields."""
    payload_text = message.get("payload")
    if not isinstance(payload_text, str):
        raise ValueError("Queue message payload is missing or not a string")

    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Payload is not valid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError("Payload JSON must be an object/dictionary")

    drone_id = _extract_drone_id(payload)
    mission_status = _extract_mission_status(payload)
    mode = _extract_mode(payload)
    position = _extract_position(payload)

    return {
        "drone_id": drone_id,
        "payload": payload,
        "mission_status": mission_status,
        "mode": mode,
        "position": position,
    }


def _resolve_active_session(
    drone_id: str,
    *,
    session_registry: ActiveSessionRegistry,
    session_source_mode: str,
) -> Any | None:
    """Resolve the active session for a drone based on the configured mode.

    In ``aws`` mode, telemetry is only accepted if the drone already has an
    active approved session. In ``local`` mode, a synthetic session is created on
    first telemetry if needed.
    """
    active_session = session_registry.get_by_drone_id(drone_id)
    if active_session is not None:
        return active_session

    if session_source_mode == "local":
        return session_registry.ensure_local_session(drone_id)

    return None


async def telemetry_worker(
    queue: asyncio.Queue[dict[str, Any]],
    state_tracker: DroneStateTracker,
    *,
    worker_name: str = "telemetry-worker",
    row_writer: MissionCsvWriter | None = None,
    row_builder: MissionRowBuilder | None = None,
    session_registry: ActiveSessionRegistry | None = None,
    session_source_mode: str = "local",
    metrics: PipelineMetrics | None = None,
    finalize_to_api: bool = False,
) -> None:
    """Consume telemetry, resolve sessions, update state, and write on finish."""
    LOGGER.info("%s started (session_source_mode=%s)", worker_name, session_source_mode)
    mission_row_builder = row_builder or MissionRowBuilder()
    active_sessions = session_registry or ActiveSessionRegistry()

    while True:
        message = await queue.get()
        processing_started_monotonic = time.monotonic()
        try:
            enqueued_monotonic = message.get("enqueued_monotonic")
            if isinstance(enqueued_monotonic, (int, float)) and metrics is not None:
                metrics.observe_queue_latency(
                    processing_started_monotonic - float(enqueued_monotonic),
                    queue.qsize(),
                )
            parsed = parse_queue_message(message)
            drone_id = parsed["drone_id"]

            if not drone_id:
                LOGGER.warning(
                    "%s skipping message with missing drone_id (topic=%s)",
                    worker_name,
                    message.get("topic"),
                )
                continue

            active_session = _resolve_active_session(
                drone_id,
                session_registry=active_sessions,
                session_source_mode=session_source_mode,
            )
            if active_session is None:
                LOGGER.info(
                    "%s ignoring telemetry for drone_id=%s because no active %s session exists",
                    worker_name,
                    drone_id,
                    session_source_mode,
                )
                continue

            last_seen = message.get("timestamp")
            if not isinstance(last_seen, str):
                last_seen = datetime.now(timezone.utc).isoformat()

            state = state_tracker.update(
                active_session.flight_session_id,
                drone_id=active_session.drone_id or drone_id,
                session_source=active_session.session_source,
                raw_message=message,
                parsed_payload=parsed["payload"],
                mission_status=parsed["mission_status"],
                mode=parsed["mode"],
                position=parsed["position"],
                last_seen=last_seen,
            )

            LOGGER.info(
                "%s updated drone_id=%s flight_session_id=%s message_count=%s mission_status=%s session_source=%s",
                worker_name,
                drone_id,
                state.flight_session_id,
                state.message_count,
                state.mission_status,
                state.session_source,
            )

            # Finalize the mission exactly once when a terminal status arrives.
            if _is_terminal_mission_status(state.mission_status) and not state.row_written:
                row = mission_row_builder.build_row(state)
                state.row_written = True

                # CSV write — always on when a row_writer is provided.
                # This is the local testing path and is independent of the API path.
                if row_writer is not None:
                    row_writer.write_row(row)
                    LOGGER.info(
                        "%s wrote final mission row for drone_id=%s flight_session_id=%s session_source=%s",
                        worker_name,
                        drone_id,
                        row.get("session_id"),
                        state.session_source,
                    )

                # API finalization — POST to SADE when running in non-local mode.
                # Enabled via --finalize-to-api; CSV and API paths are independent.
                if finalize_to_api:
                    fin_payload = build_finalization_payload(state)
                    await post_tracker_session_finalized(fin_payload)

                if metrics is not None:
                    metrics.observe_final_row_written()
                state_tracker.pop(state.flight_session_id)
                active_sessions.complete(state.flight_session_id)

            if metrics is not None:
                metrics.observe_processed(time.monotonic() - processing_started_monotonic)

        except asyncio.CancelledError:
            LOGGER.info("%s cancelled", worker_name)
            raise
        except Exception as exc:  # noqa: BLE001
            if metrics is not None:
                metrics.observe_failure(time.monotonic() - processing_started_monotonic)
            LOGGER.warning(
                "%s failed to process message (topic=%s): %s",
                worker_name,
                message.get("topic"),
                exc,
            )
        finally:
            queue.task_done()
