"""Unit tests for app.ingestion.workers (parsing and session resolution)."""

from __future__ import annotations

import json

import pytest

from app.ingestion.workers import (
    TERMINAL_MISSION_STATUSES,
    _extract_drone_id,
    _extract_mode,
    _extract_mission_status,
    _extract_position,
    _is_terminal_mission_status,
    _resolve_active_session,
    parse_queue_message,
)
from app.monitoring.active_session_registry import (
    ActiveFlightSession,
    ActiveSessionRegistry,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _wrap_payload(payload: dict) -> dict:
    """Wrap a telemetry payload dict into a queue message envelope."""
    return {"payload": json.dumps(payload), "topic": "update_drone"}


# ── _extract_drone_id() ─────────────────────────────────────────────────────


class TestExtractDroneId:
    def test_from_drone_id(self):
        assert _extract_drone_id({"drone_id": "d1"}) == "d1"

    def test_from_droneId(self):
        assert _extract_drone_id({"droneId": "d1"}) == "d1"

    def test_from_uavid(self):
        assert _extract_drone_id({"uavid": "d1"}) == "d1"

    def test_from_uavID(self):
        assert _extract_drone_id({"uavID": "d1"}) == "d1"

    def test_from_uav_id(self):
        assert _extract_drone_id({"uav_id": "d1"}) == "d1"

    def test_priority(self):
        # drone_id is checked first, so it wins over uavid
        assert _extract_drone_id({"drone_id": "first", "uavid": "second"}) == "first"

    def test_missing(self):
        assert _extract_drone_id({}) is None


# ── _extract_position() ─────────────────────────────────────────────────────


class TestExtractPosition:
    def test_full(self):
        payload = {
            "status": {
                "location": {
                    "latitude": 39.77,
                    "longitude": -86.16,
                    "altitude": 100.0,
                }
            }
        }
        pos = _extract_position(payload)

        assert pos["latitude"] == 39.77
        assert pos["longitude"] == -86.16
        assert pos["altitude"] == 100.0
        assert pos["raw"] == payload["status"]["location"]

    def test_short_names(self):
        payload = {
            "status": {
                "location": {"lat": 39.77, "lon": -86.16, "alt": 100.0}
            }
        }
        pos = _extract_position(payload)

        assert pos["latitude"] == 39.77
        assert pos["longitude"] == -86.16
        assert pos["altitude"] == 100.0

    def test_lng_variant(self):
        payload = {
            "status": {
                "location": {"lat": 39.77, "lng": -86.16}
            }
        }
        pos = _extract_position(payload)

        assert pos["latitude"] == 39.77
        assert pos["longitude"] == -86.16

    def test_partial(self):
        payload = {
            "status": {
                "location": {"latitude": 39.77}
            }
        }
        pos = _extract_position(payload)

        assert pos is not None
        assert pos["latitude"] == 39.77
        assert pos["longitude"] is None
        assert pos["altitude"] is None

    def test_missing(self):
        assert _extract_position({}) is None

    def test_status_not_dict(self):
        assert _extract_position({"status": "flying"}) is None


# ── _extract_mission_status() ────────────────────────────────────────────────


class TestExtractMissionStatus:
    def test_from_top_level(self):
        assert _extract_mission_status({"mission_status": "on_mission"}) == "on_mission"

    def test_from_nested_status(self):
        payload = {"status": {"status": "tracking"}}
        assert _extract_mission_status(payload) == "tracking"

    def test_top_level_takes_priority(self):
        payload = {"mission_status": "complete", "status": {"status": "tracking"}}
        assert _extract_mission_status(payload) == "complete"

    def test_missing(self):
        assert _extract_mission_status({}) is None

    def test_status_not_dict(self):
        assert _extract_mission_status({"status": "flying"}) is None


# ── _extract_mode() ─────────────────────────────────────────────────────────


class TestExtractMode:
    def test_from_top_level(self):
        assert _extract_mode({"mode": "AUTO"}) == "AUTO"

    def test_from_nested_status(self):
        assert _extract_mode({"status": {"mode": "MANUAL"}}) == "MANUAL"

    def test_top_level_takes_priority(self):
        payload = {"mode": "AUTO", "status": {"mode": "MANUAL"}}
        assert _extract_mode(payload) == "AUTO"

    def test_missing(self):
        assert _extract_mode({}) is None


# ── _is_terminal_mission_status() ────────────────────────────────────────────


class TestIsTerminalMissionStatus:
    @pytest.mark.parametrize("status", sorted(TERMINAL_MISSION_STATUSES))
    def test_terminal_statuses(self, status: str):
        assert _is_terminal_mission_status(status) is True

    @pytest.mark.parametrize("status", ["Mission_Completed", "DONE", "Finished", "COMPLETE"])
    def test_case_insensitive(self, status: str):
        assert _is_terminal_mission_status(status) is True

    def test_whitespace_stripped(self):
        assert _is_terminal_mission_status("  completed  ") is True

    @pytest.mark.parametrize("status", ["on_mission", "tracking", "takeoff", "hovering"])
    def test_non_terminal(self, status: str):
        assert _is_terminal_mission_status(status) is False

    def test_none(self):
        assert _is_terminal_mission_status(None) is False


# ── parse_queue_message() ────────────────────────────────────────────────────


class TestParseQueueMessage:
    def test_valid_message(self):
        payload = {
            "uavid": "drone-01",
            "mission_status": "on_mission",
            "mode": "AUTO",
            "status": {
                "location": {
                    "latitude": 39.77,
                    "longitude": -86.16,
                    "altitude": 110.0,
                }
            },
        }
        message = _wrap_payload(payload)
        result = parse_queue_message(message)

        assert result["drone_id"] == "drone-01"
        assert result["mission_status"] == "on_mission"
        assert result["mode"] == "AUTO"
        assert result["position"]["latitude"] == 39.77
        assert result["payload"] == payload

    def test_missing_payload(self):
        with pytest.raises(ValueError, match="payload is missing"):
            parse_queue_message({"topic": "update_drone"})

    def test_payload_not_string(self):
        with pytest.raises(ValueError, match="payload is missing or not a string"):
            parse_queue_message({"payload": 123})

    def test_invalid_json(self):
        with pytest.raises(ValueError, match="not valid JSON"):
            parse_queue_message({"payload": "not json {{"})

    def test_payload_not_dict(self):
        with pytest.raises(ValueError, match="must be an object"):
            parse_queue_message({"payload": "[1, 2, 3]"})

    def test_minimal_payload(self):
        result = parse_queue_message({"payload": "{}"})

        assert result["drone_id"] is None
        assert result["mission_status"] is None
        assert result["mode"] is None
        assert result["position"] is None
        assert result["payload"] == {}


# ── _resolve_active_session() ────────────────────────────────────────────────


class TestResolveActiveSession:
    def test_local_mode_creates_session(self):
        reg = ActiveSessionRegistry()
        session = _resolve_active_session(
            "drone-01",
            session_registry=reg,
            session_source_mode="local",
        )

        assert session is not None
        assert session.drone_id == "drone-01"
        assert session.session_source == "local"

    def test_local_mode_reuses_existing(self):
        reg = ActiveSessionRegistry()
        first = _resolve_active_session(
            "drone-01",
            session_registry=reg,
            session_source_mode="local",
        )
        second = _resolve_active_session(
            "drone-01",
            session_registry=reg,
            session_source_mode="local",
        )

        assert first.flight_session_id == second.flight_session_id

    def test_aws_mode_returns_existing(self):
        reg = ActiveSessionRegistry()
        pre_registered = ActiveFlightSession(
            flight_session_id="flight-001",
            drone_id="drone-01",
            session_source="aws",
        )
        reg.register(pre_registered)

        session = _resolve_active_session(
            "drone-01",
            session_registry=reg,
            session_source_mode="aws",
        )

        assert session is pre_registered

    def test_aws_mode_returns_none_for_unknown(self):
        reg = ActiveSessionRegistry()
        session = _resolve_active_session(
            "drone-01",
            session_registry=reg,
            session_source_mode="aws",
        )

        assert session is None
