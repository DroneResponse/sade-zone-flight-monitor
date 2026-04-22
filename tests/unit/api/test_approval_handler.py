"""Unit tests for app.api.approval_handler."""

from __future__ import annotations

import logging

import pytest
from pydantic import ValidationError

from app.api.approval_handler import (
    EntryApprovalPayload,
    RegisterSessionPayload,
    process_approval,
    process_register_session,
)
from app.monitoring.active_session_registry import ActiveSessionRegistry


# ── RegisterSessionPayload validation ────────────────────────────────────────


class TestRegisterSessionPayload:
    def test_minimal(self):
        payload = RegisterSessionPayload(flight_session_id="flight-001")

        assert payload.flight_session_id == "flight-001"
        assert payload.drone_id is None
        assert payload.pilot_id is None
        assert payload.requested_operation is None
        assert payload.test_overrides is None
        assert payload.submitted_at is None

    def test_full(self):
        payload = RegisterSessionPayload(
            flight_session_id="flight-001",
            drone_id="drone-01",
            pilot_id="pilot-01",
            organization_id="org-01",
            sade_zone_id="zone-01",
            requested_entry_time="2026-01-01T00:00:00Z",
            requested_exit_time="2026-01-01T01:00:00Z",
            requested_operation={"operation_type": "INSPECTION"},
            test_overrides={"actual_start_time": "2026-01-01T00:05:00Z"},
            submitted_at="2026-01-01T00:00:00Z",
        )

        assert payload.drone_id == "drone-01"
        assert payload.requested_operation == {"operation_type": "INSPECTION"}
        assert payload.test_overrides is not None
        assert payload.submitted_at == "2026-01-01T00:00:00Z"

    def test_missing_flight_session_id_raises(self):
        with pytest.raises(ValidationError):
            RegisterSessionPayload()


# ── process_register_session() ───────────────────────────────────────────────


class TestProcessRegisterSession:
    def test_success(self):
        reg = ActiveSessionRegistry()
        payload = RegisterSessionPayload(
            flight_session_id="flight-001",
            drone_id="drone-01",
        )
        result = process_register_session(payload, reg)

        assert result["action"] == "registered"
        assert result["flight_session_id"] == "flight-001"
        assert result["drone_id"] == "drone-01"

    def test_stored_in_registry(self):
        reg = ActiveSessionRegistry()
        payload = RegisterSessionPayload(
            flight_session_id="flight-001",
            drone_id="drone-01",
        )
        process_register_session(payload, reg)

        assert reg.get_by_flight_session_id("flight-001") is not None
        assert reg.get_by_drone_id("drone-01") is not None

    def test_sets_decision_registered(self):
        reg = ActiveSessionRegistry()
        payload = RegisterSessionPayload(
            flight_session_id="flight-001",
            drone_id="drone-01",
        )
        process_register_session(payload, reg)

        session = reg.get_by_flight_session_id("flight-001")
        assert session.decision == "REGISTERED"

    def test_sets_session_source_aws(self):
        reg = ActiveSessionRegistry()
        payload = RegisterSessionPayload(
            flight_session_id="flight-001",
        )
        process_register_session(payload, reg)

        session = reg.get_by_flight_session_id("flight-001")
        assert session.session_source == "aws"

    def test_stores_source_payload(self):
        reg = ActiveSessionRegistry()
        payload = RegisterSessionPayload(
            flight_session_id="flight-001",
            drone_id="drone-01",
        )
        process_register_session(payload, reg)

        session = reg.get_by_flight_session_id("flight-001")
        assert session.source_payload["flight_session_id"] == "flight-001"
        assert session.source_payload["drone_id"] == "drone-01"

    def test_stores_optional_fields(self):
        reg = ActiveSessionRegistry()
        payload = RegisterSessionPayload(
            flight_session_id="flight-001",
            requested_operation={"operation_type": "SURVEY"},
            test_overrides={"actual_start_time": "2026-01-01T00:05:00Z"},
            submitted_at="2026-01-01T00:00:00Z",
        )
        process_register_session(payload, reg)

        session = reg.get_by_flight_session_id("flight-001")
        assert session.requested_operation == {"operation_type": "SURVEY"}
        assert session.test_overrides is not None
        assert session.submitted_at == "2026-01-01T00:00:00Z"

    def test_duplicate_drone_rejected(self):
        reg = ActiveSessionRegistry()
        payload_a = RegisterSessionPayload(
            flight_session_id="flight-A",
            drone_id="drone-01",
        )
        payload_b = RegisterSessionPayload(
            flight_session_id="flight-B",
            drone_id="drone-01",
        )
        process_register_session(payload_a, reg)
        result = process_register_session(payload_b, reg)

        assert result["action"] == "rejected"
        assert "reason" in result

    def test_without_drone_id(self):
        reg = ActiveSessionRegistry()
        payload = RegisterSessionPayload(
            flight_session_id="flight-001",
        )
        result = process_register_session(payload, reg)

        assert result["action"] == "registered"
        assert reg.get_by_flight_session_id("flight-001") is not None


# ── EntryApprovalPayload validation (deprecated) ────────────────────────────


class TestEntryApprovalPayload:
    def test_with_decision(self):
        payload = EntryApprovalPayload(
            evaluation_series_id="eval-001",
            decision="APPROVED",
        )
        assert payload.decision == "APPROVED"

    def test_status_fallback(self):
        payload = EntryApprovalPayload(
            evaluation_series_id="eval-001",
            status="APPROVED",
        )
        # status is normalized into decision by the model validator
        assert payload.decision == "APPROVED"

    def test_decision_takes_precedence(self):
        payload = EntryApprovalPayload(
            evaluation_series_id="eval-001",
            decision="DENIED",
            status="APPROVED",
        )
        assert payload.decision == "DENIED"

    def test_neither_decision_nor_status_raises(self):
        with pytest.raises(ValidationError, match="decision.*status"):
            EntryApprovalPayload(evaluation_series_id="eval-001")

    def test_resolved_decision_uppercased(self):
        payload = EntryApprovalPayload(
            evaluation_series_id="eval-001",
            decision="approved",
        )
        assert payload.resolved_decision == "APPROVED"


# ── process_approval() (deprecated) ─────────────────────────────────────────


class TestProcessApproval:
    def test_approved_registers(self):
        reg = ActiveSessionRegistry()
        payload = EntryApprovalPayload(
            evaluation_series_id="eval-001",
            decision="APPROVED",
            flight_session_id="flight-001",
            drone_id="drone-01",
        )
        result = process_approval(payload, reg)

        assert result["action"] == "registered"
        assert result["flight_session_id"] == "flight-001"

    def test_approved_constraints_registers(self):
        reg = ActiveSessionRegistry()
        payload = EntryApprovalPayload(
            evaluation_series_id="eval-001",
            decision="APPROVED_CONSTRAINTS",
            flight_session_id="flight-001",
            drone_id="drone-01",
        )
        result = process_approval(payload, reg)

        assert result["action"] == "registered"

    def test_denied_ignored(self):
        reg = ActiveSessionRegistry()
        payload = EntryApprovalPayload(
            evaluation_series_id="eval-001",
            decision="DENIED",
        )
        result = process_approval(payload, reg)

        assert result["action"] == "ignored"
        assert reg.count() == 0

    def test_approved_missing_flight_session_id_rejected(self):
        reg = ActiveSessionRegistry()
        payload = EntryApprovalPayload(
            evaluation_series_id="eval-001",
            decision="APPROVED",
            # no flight_session_id
        )
        result = process_approval(payload, reg)

        assert result["action"] == "rejected"
        assert "flight_session_id" in result["reason"].lower()

    def test_duplicate_drone_rejected(self):
        reg = ActiveSessionRegistry()
        payload_a = EntryApprovalPayload(
            evaluation_series_id="eval-A",
            decision="APPROVED",
            flight_session_id="flight-A",
            drone_id="drone-01",
        )
        payload_b = EntryApprovalPayload(
            evaluation_series_id="eval-B",
            decision="APPROVED",
            flight_session_id="flight-B",
            drone_id="drone-01",
        )
        process_approval(payload_a, reg)
        result = process_approval(payload_b, reg)

        assert result["action"] == "rejected"

    def test_logs_deprecation_warning(self, caplog):
        reg = ActiveSessionRegistry()
        payload = EntryApprovalPayload(
            evaluation_series_id="eval-001",
            decision="DENIED",
        )

        with caplog.at_level(logging.WARNING, logger="app.api.approval_handler"):
            process_approval(payload, reg)

        assert "Deprecated" in caplog.text
        assert "/entry-approval" in caplog.text
