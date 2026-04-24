"""Unit tests for app.sending.tracker_finalizer (payload builders and helpers)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest

from app.monitoring.state_tracker import DroneState
from app.sending.tracker_finalizer import (
    TRACKER_FINALIZED_URL_ENV_VAR,
    _log_finalization_response,
    _to_utc_z,
    build_finalization_payload,
    build_stub_finalization_payload,
    get_tracker_finalized_url,
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
    distance_flown_m: float = 1250.0,
    exit_requested_at: str | None = None,
    exit_reason: str | None = None,
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
        distance_flown_m=distance_flown_m,
        message_count=10,
        exit_requested_at=exit_requested_at,
        exit_reason=exit_reason,
    )


def _utc_now_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _first_flight_segment(payload: dict) -> dict:
    return next(ev for ev in payload["events"] if ev["type"] == "FLIGHT_SEGMENT")


# ── get_tracker_finalized_url() ──────────────────────────────────────────────


class TestGetTrackerFinalizedUrl:
    def test_returns_env_var_when_set(self, monkeypatch):
        monkeypatch.setenv(TRACKER_FINALIZED_URL_ENV_VAR, "http://mock.example.com/tracker-session-finalized")
        assert get_tracker_finalized_url() == "http://mock.example.com/tracker-session-finalized"

    def test_raises_when_unset(self, monkeypatch):
        monkeypatch.delenv(TRACKER_FINALIZED_URL_ENV_VAR, raising=False)
        with pytest.raises(RuntimeError, match=TRACKER_FINALIZED_URL_ENV_VAR):
            get_tracker_finalized_url()

    def test_raises_when_empty(self, monkeypatch):
        monkeypatch.setenv(TRACKER_FINALIZED_URL_ENV_VAR, "")
        with pytest.raises(RuntimeError, match=TRACKER_FINALIZED_URL_ENV_VAR):
            get_tracker_finalized_url()


# ── build_finalization_payload() ─────────────────────────────────────────────


class TestBuildFinalizationPayload:
    def test_top_level_structure(self):
        state = _make_drone_state()
        payload = build_finalization_payload(state)

        assert set(payload.keys()) == {
            "flight_session_id",
            "report_time_utc",
            "telemetry_summary",
            "events",
        }

    def test_telemetry_summary_has_only_allowed_keys(self):
        state = _make_drone_state()
        payload = build_finalization_payload(state)

        # Per SADE_CONTRACT.md, telemetry_summary is limited to these three.
        # Guard against regressions that would add battery_* back at the top.
        assert set(payload["telemetry_summary"].keys()) == {
            "altitude_min_m",
            "altitude_max_m",
            "distance_flown_m",
        }

    def test_flight_session_id_from_state(self):
        state = _make_drone_state(flight_session_id="flight-xyz")
        payload = build_finalization_payload(state)

        assert payload["flight_session_id"] == "flight-xyz"

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

    def test_distance_flown_from_state(self):
        state = _make_drone_state(distance_flown_m=1250.5)
        payload = build_finalization_payload(state)

        assert payload["telemetry_summary"]["distance_flown_m"] == 1250.5

    def test_report_time_utc_is_current_utc(self):
        before = _utc_now_z()
        state = _make_drone_state()
        payload = build_finalization_payload(state)
        after = _utc_now_z()

        assert before <= payload["report_time_utc"] <= after


class TestFlightSegmentEvent:
    def test_single_segment_emitted(self):
        state = _make_drone_state()
        payload = build_finalization_payload(state)

        segments = [ev for ev in payload["events"] if ev["type"] == "FLIGHT_SEGMENT"]
        assert len(segments) == 1

    def test_segment_times_normalized_to_utc_z(self):
        state = _make_drone_state(
            first_seen="2026-03-09T18:00:00+00:00",
            last_seen="2026-03-09T19:03:00+00:00",
        )
        segment = _first_flight_segment(build_finalization_payload(state))

        assert segment["time_in_utc"] == "2026-03-09T18:00:00Z"
        assert segment["time_out_utc"] == "2026-03-09T19:03:00Z"

    def test_battery_state_wraps_single_slot_a(self):
        state = _make_drone_state(voltage_in=24.8, voltage_out=23.9)
        segment = _first_flight_segment(build_finalization_payload(state))

        assert segment["battery_state_in"]["slots"] == [{"slot_id": "A", "voltage_v": 24.8}]
        assert segment["battery_state_out"]["slots"] == [{"slot_id": "A", "voltage_v": 23.9}]

    def test_battery_state_voltage_none_defaults_to_zero(self):
        state = _make_drone_state(voltage_in=None, voltage_out=None)
        segment = _first_flight_segment(build_finalization_payload(state))

        assert segment["battery_state_in"]["slots"][0]["voltage_v"] == 0.0
        assert segment["battery_state_out"]["slots"][0]["voltage_v"] == 0.0

    def test_system_charge_pct_is_zero_placeholder(self):
        # Known gap: telemetry carries voltage only.  Change to null (or real
        # value) once firmware emits a battery-percentage field.
        state = _make_drone_state()
        segment = _first_flight_segment(build_finalization_payload(state))

        assert segment["battery_state_in"]["system_charge_pct"] == 0.0
        assert segment["battery_state_out"]["system_charge_pct"] == 0.0


class TestExitRequestEvent:
    def test_no_exit_requested_yields_no_exit_event(self):
        state = _make_drone_state()
        payload = build_finalization_payload(state)

        exit_events = [ev for ev in payload["events"] if ev["type"] == "EXIT_REQUEST"]
        assert exit_events == []

    def test_exit_requested_yields_one_exit_event(self):
        state = _make_drone_state(
            exit_requested_at="2026-03-09T18:35:00+00:00",
            exit_reason="Returning home early",
        )
        payload = build_finalization_payload(state)

        exit_events = [ev for ev in payload["events"] if ev["type"] == "EXIT_REQUEST"]
        assert len(exit_events) == 1
        assert exit_events[0] == {
            "type": "EXIT_REQUEST",
            "time_utc": "2026-03-09T18:35:00Z",
            "reason": "Returning home early",
        }

    def test_exit_event_reason_defaults_to_unspecified(self):
        state = _make_drone_state(
            exit_requested_at="2026-03-09T18:35:00+00:00",
            exit_reason=None,
        )
        payload = build_finalization_payload(state)

        exit_events = [ev for ev in payload["events"] if ev["type"] == "EXIT_REQUEST"]
        assert exit_events[0]["reason"] == "unspecified"


# ── build_stub_finalization_payload() ────────────────────────────────────────


class TestBuildStubFinalizationPayload:
    def test_top_level_structure(self):
        payload = build_stub_finalization_payload("flight-stub", {})

        assert set(payload.keys()) == {
            "flight_session_id",
            "report_time_utc",
            "telemetry_summary",
            "events",
        }

    def test_flight_session_id_passed_through(self):
        payload = build_stub_finalization_payload("my-flight-id", {})

        assert payload["flight_session_id"] == "my-flight-id"

    def test_telemetry_values_from_overrides(self):
        overrides = {
            "telemetry_summary": {
                "altitude_min_m": 10.0,
                "altitude_max_m": 90.0,
                "distance_flown_m": 1500.0,
            },
        }
        payload = build_stub_finalization_payload("f1", overrides)
        summary = payload["telemetry_summary"]

        assert summary["altitude_min_m"] == 10.0
        assert summary["altitude_max_m"] == 90.0
        assert summary["distance_flown_m"] == 1500.0

    def test_telemetry_summary_limited_to_three_keys(self):
        payload = build_stub_finalization_payload("f1", {})

        assert set(payload["telemetry_summary"].keys()) == {
            "altitude_min_m",
            "altitude_max_m",
            "distance_flown_m",
        }

    def test_missing_telemetry_summary_defaults_to_zeros(self):
        payload = build_stub_finalization_payload("f1", {})
        summary = payload["telemetry_summary"]

        assert summary["altitude_min_m"] == 0.0
        assert summary["altitude_max_m"] == 0.0
        assert summary["distance_flown_m"] == 0.0

    def test_events_passed_through_from_overrides(self):
        overrides = {
            "events": [
                {
                    "type": "FLIGHT_SEGMENT",
                    "time_in_utc": "2026-03-09T18:05:00Z",
                    "time_out_utc": "2026-03-09T18:41:00Z",
                },
            ],
        }
        payload = build_stub_finalization_payload("f1", overrides)

        assert len(payload["events"]) == 1
        assert payload["events"][0]["time_in_utc"] == "2026-03-09T18:05:00Z"
        assert payload["events"][0]["time_out_utc"] == "2026-03-09T18:41:00Z"

    def test_missing_events_synthesizes_single_flight_segment(self):
        before = _utc_now_z()
        payload = build_stub_finalization_payload("f1", {})
        after = _utc_now_z()

        assert len(payload["events"]) == 1
        segment = payload["events"][0]
        assert segment["type"] == "FLIGHT_SEGMENT"
        assert before <= segment["time_in_utc"] <= after
        assert before <= segment["time_out_utc"] <= after
        assert segment["battery_state_in"]["slots"][0]["slot_id"] == "A"
        assert segment["battery_state_out"]["slots"][0]["slot_id"] == "A"

    def test_report_time_utc_is_current_utc(self):
        before = _utc_now_z()
        payload = build_stub_finalization_payload("f1", {})
        after = _utc_now_z()

        assert before <= payload["report_time_utc"] <= after


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
