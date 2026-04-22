"""Unit tests for app.sending.tracker_finalizer (payload builders and helpers)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest

from app.monitoring.state_tracker import DroneState
from app.sending.tracker_finalizer import (
    _log_finalization_response,
    _to_utc_z,
    build_finalization_payload,
    build_stub_finalization_payload,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_drone_state(
    *,
    flight_session_id: str = "flight-001",
    first_seen: str = "2026-01-01T12:00:00+00:00",
    last_seen: str = "2026-01-01T12:45:00+00:00",
    min_altitude: float | None = 15.0,
    max_altitude: float | None = 92.0,
    voltage_in: float | None = 16.4,
    voltage_out: float | None = 14.9,
) -> DroneState:
    return DroneState(
        flight_session_id=flight_session_id,
        drone_id="drone-01",
        session_source="aws",
        first_seen=first_seen,
        last_seen=last_seen,
        latest_raw_message={},
        latest_parsed_payload={},
        min_altitude=min_altitude,
        max_altitude=max_altitude,
        voltage_in=voltage_in,
        voltage_out=voltage_out,
        message_count=10,
    )


def _utc_now_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── build_finalization_payload() ─────────────────────────────────────────────


class TestBuildFinalizationPayload:
    def test_basic_payload_structure(self):
        state = _make_drone_state()
        payload = build_finalization_payload(state)

        assert "flight_session_id" in payload
        assert "report_time" in payload
        assert "actual_start_time" in payload
        assert "actual_end_time" in payload
        assert "telemetry_summary" in payload

        summary = payload["telemetry_summary"]
        for key in (
            "altitude_min_m", "altitude_max_m",
            "battery_start_pct", "battery_end_pct",
            "battery_voltage_start_v", "battery_voltage_end_v",
        ):
            assert key in summary

    def test_flight_session_id_from_state(self):
        state = _make_drone_state(flight_session_id="flight-xyz")
        payload = build_finalization_payload(state)

        assert payload["flight_session_id"] == "flight-xyz"

    def test_times_normalized_to_utc_z(self):
        state = _make_drone_state(
            first_seen="2026-03-09T18:00:00+00:00",
            last_seen="2026-03-09T19:03:00+00:00",
        )
        payload = build_finalization_payload(state)

        assert payload["actual_start_time"] == "2026-03-09T18:00:00Z"
        assert payload["actual_end_time"] == "2026-03-09T19:03:00Z"

    def test_altitude_from_state(self):
        state = _make_drone_state(min_altitude=12.0, max_altitude=94.5)
        payload = build_finalization_payload(state)

        assert payload["telemetry_summary"]["altitude_min_m"] == 12.0
        assert payload["telemetry_summary"]["altitude_max_m"] == 94.5

    def test_altitude_none_defaults_to_zero(self):
        state = _make_drone_state(min_altitude=None, max_altitude=None)
        payload = build_finalization_payload(state)

        assert payload["telemetry_summary"]["altitude_min_m"] == 0.0
        assert payload["telemetry_summary"]["altitude_max_m"] == 0.0

    def test_voltage_from_state(self):
        state = _make_drone_state(voltage_in=16.2, voltage_out=14.9)
        payload = build_finalization_payload(state)

        assert payload["telemetry_summary"]["battery_voltage_start_v"] == 16.2
        assert payload["telemetry_summary"]["battery_voltage_end_v"] == 14.9

    def test_voltage_none_defaults_to_zero(self):
        state = _make_drone_state(voltage_in=None, voltage_out=None)
        payload = build_finalization_payload(state)

        assert payload["telemetry_summary"]["battery_voltage_start_v"] == 0.0
        assert payload["telemetry_summary"]["battery_voltage_end_v"] == 0.0

    def test_battery_pct_always_zero(self):
        state = _make_drone_state()
        payload = build_finalization_payload(state)

        assert payload["telemetry_summary"]["battery_start_pct"] == 0.0
        assert payload["telemetry_summary"]["battery_end_pct"] == 0.0

    def test_report_time_is_current_utc(self):
        before = _utc_now_z()
        state = _make_drone_state()
        payload = build_finalization_payload(state)
        after = _utc_now_z()

        assert before <= payload["report_time"] <= after


# ── build_stub_finalization_payload() ────────────────────────────────────────


class TestBuildStubFinalizationPayload:
    def test_basic_stub_structure(self):
        overrides = {
            "actual_start_time": "2026-01-01T18:05:00Z",
            "actual_end_time": "2026-01-01T18:41:00Z",
            "telemetry_summary": {
                "altitude_min_m": 12.0,
                "altitude_max_m": 88.0,
                "battery_start_pct": 98.0,
                "battery_end_pct": 61.0,
            },
        }
        payload = build_stub_finalization_payload("flight-stub", overrides)

        assert "flight_session_id" in payload
        assert "report_time" in payload
        assert "actual_start_time" in payload
        assert "actual_end_time" in payload
        assert "telemetry_summary" in payload

    def test_flight_session_id_passed_through(self):
        payload = build_stub_finalization_payload("my-flight-id", {})

        assert payload["flight_session_id"] == "my-flight-id"

    def test_times_from_overrides(self):
        overrides = {
            "actual_start_time": "2026-04-21T18:05:00Z",
            "actual_end_time": "2026-04-21T18:41:00Z",
        }
        payload = build_stub_finalization_payload("f1", overrides)

        assert payload["actual_start_time"] == "2026-04-21T18:05:00Z"
        assert payload["actual_end_time"] == "2026-04-21T18:41:00Z"

    def test_telemetry_values_from_overrides(self):
        overrides = {
            "telemetry_summary": {
                "altitude_min_m": 10.0,
                "altitude_max_m": 90.0,
                "battery_start_pct": 97.0,
                "battery_end_pct": 58.0,
            },
        }
        payload = build_stub_finalization_payload("f1", overrides)
        summary = payload["telemetry_summary"]

        assert summary["altitude_min_m"] == 10.0
        assert summary["altitude_max_m"] == 90.0
        assert summary["battery_start_pct"] == 97.0
        assert summary["battery_end_pct"] == 58.0

    def test_missing_times_default_to_utc_now(self):
        before = _utc_now_z()
        payload = build_stub_finalization_payload("f1", {})
        after = _utc_now_z()

        assert before <= payload["actual_start_time"] <= after
        assert before <= payload["actual_end_time"] <= after

    def test_missing_telemetry_summary_defaults_to_zeros(self):
        payload = build_stub_finalization_payload("f1", {})
        summary = payload["telemetry_summary"]

        assert summary["altitude_min_m"] == 0.0
        assert summary["altitude_max_m"] == 0.0
        assert summary["battery_start_pct"] == 0.0
        assert summary["battery_end_pct"] == 0.0
        assert summary["battery_voltage_start_v"] == 0.0
        assert summary["battery_voltage_end_v"] == 0.0

    def test_partial_telemetry_summary(self):
        overrides = {
            "telemetry_summary": {
                "altitude_max_m": 75.0,
            },
        }
        payload = build_stub_finalization_payload("f1", overrides)
        summary = payload["telemetry_summary"]

        assert summary["altitude_max_m"] == 75.0
        # Everything else defaults to 0.0
        assert summary["altitude_min_m"] == 0.0
        assert summary["battery_start_pct"] == 0.0
        assert summary["battery_voltage_start_v"] == 0.0

    def test_voltage_from_overrides(self):
        overrides = {
            "telemetry_summary": {
                "battery_voltage_start_v": 16.2,
                "battery_voltage_end_v": 14.9,
            },
        }
        payload = build_stub_finalization_payload("f1", overrides)
        summary = payload["telemetry_summary"]

        assert summary["battery_voltage_start_v"] == 16.2
        assert summary["battery_voltage_end_v"] == 14.9


# ── _to_utc_z() ─────────────────────────────────────────────────────────────


class TestToUtcZ:
    def test_converts_plus_zero_offset(self):
        assert _to_utc_z("2026-01-01T12:00:00+00:00") == "2026-01-01T12:00:00Z"

    def test_converts_isoformat(self):
        assert _to_utc_z("2026-03-09T18:05:00") == "2026-03-09T18:05:00Z"

    def test_already_z_format(self):
        result = _to_utc_z("2026-01-01T12:00:00Z")
        assert result == "2026-01-01T12:00:00Z"

    def test_none_defaults_to_utc_now(self):
        before = _utc_now_z()
        result = _to_utc_z(None)
        after = _utc_now_z()

        assert result.endswith("Z")
        assert before <= result <= after

    def test_empty_string_defaults_to_utc_now(self):
        before = _utc_now_z()
        result = _to_utc_z("")
        after = _utc_now_z()

        assert result.endswith("Z")
        assert before <= result <= after

    def test_invalid_format_passthrough(self):
        assert _to_utc_z("not-a-timestamp") == "not-a-timestamp"


# ── _log_finalization_response() ─────────────────────────────────────────────


class TestLogFinalizationResponse:
    def test_exited_logs_info(self, caplog):
        with caplog.at_level(logging.INFO, logger="app.sending.tracker_finalizer"):
            _log_finalization_response({
                "status": "EXITED",
                "flight_session_id": "f1",
                "reputation_record_id": "rep-001",
            })

        assert "Tracker session finalized" in caplog.text
        assert "rep-001" in caplog.text

    def test_failed_logs_warning(self, caplog):
        with caplog.at_level(logging.WARNING, logger="app.sending.tracker_finalizer"):
            _log_finalization_response({
                "status": "FAILED",
                "flight_session_id": "f1",
                "reason": "No session found",
            })

        assert "business failure" in caplog.text
        assert "No session found" in caplog.text

    def test_unexpected_status_logs_warning(self, caplog):
        with caplog.at_level(logging.WARNING, logger="app.sending.tracker_finalizer"):
            _log_finalization_response({
                "status": "UNKNOWN",
                "flight_session_id": "f1",
                "reason": "something odd",
            })

        assert "unexpected status" in caplog.text
        assert "UNKNOWN" in caplog.text
