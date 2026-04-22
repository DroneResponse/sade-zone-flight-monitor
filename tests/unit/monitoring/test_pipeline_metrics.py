"""Unit tests for app.monitoring.pipeline_metrics."""

from __future__ import annotations

from app.monitoring.pipeline_metrics import PipelineMetrics


# ── Fresh state ──────────────────────────────────────────────────────────────


class TestInitialState:
    def test_initial_counters_zero(self):
        m = PipelineMetrics()

        assert m.enqueued_messages == 0
        assert m.dropped_messages == 0
        assert m.processed_messages == 0
        assert m.failed_messages == 0
        assert m.final_rows_written == 0
        assert m.max_queue_depth == 0

    def test_initial_snapshot_all_zeros(self):
        m = PipelineMetrics()
        snap = m.snapshot(0)

        assert snap.enqueued_messages == 0
        assert snap.dropped_messages == 0
        assert snap.processed_messages == 0
        assert snap.failed_messages == 0
        assert snap.final_rows_written == 0
        assert snap.current_queue_depth == 0
        assert snap.max_queue_depth == 0
        assert snap.average_queue_latency_ms == 0.0
        assert snap.max_queue_latency_ms == 0.0
        assert snap.average_processing_ms == 0.0
        assert snap.max_processing_ms == 0.0


# ── record_enqueue() ─────────────────────────────────────────────────────────


class TestRecordEnqueue:
    def test_increments_counter(self):
        m = PipelineMetrics()
        m.record_enqueue(1)
        m.record_enqueue(2)
        m.record_enqueue(3)

        assert m.enqueued_messages == 3

    def test_tracks_max_queue_depth(self):
        m = PipelineMetrics()
        m.record_enqueue(5)
        m.record_enqueue(10)
        m.record_enqueue(3)

        assert m.max_queue_depth == 10


# ── record_drop() ────────────────────────────────────────────────────────────


class TestRecordDrop:
    def test_increments_counter(self):
        m = PipelineMetrics()
        m.record_drop(100)
        m.record_drop(100)

        assert m.dropped_messages == 2

    def test_tracks_max_queue_depth(self):
        m = PipelineMetrics()
        m.record_enqueue(5)
        m.record_drop(100)

        assert m.max_queue_depth == 100


# ── observe_queue_latency() ──────────────────────────────────────────────────


class TestObserveQueueLatency:
    def test_average(self):
        m = PipelineMetrics()
        m.observe_queue_latency(0.1, queue_depth=1)
        m.observe_queue_latency(0.3, queue_depth=1)

        snap = m.snapshot(0)
        assert snap.average_queue_latency_ms == pytest.approx(200.0)

    def test_max(self):
        m = PipelineMetrics()
        m.observe_queue_latency(0.1, queue_depth=1)
        m.observe_queue_latency(0.5, queue_depth=1)
        m.observe_queue_latency(0.2, queue_depth=1)

        snap = m.snapshot(0)
        assert snap.max_queue_latency_ms == pytest.approx(500.0)

    def test_negative_clamped_to_zero(self):
        m = PipelineMetrics()
        m.observe_queue_latency(-5.0, queue_depth=1)

        snap = m.snapshot(0)
        assert snap.average_queue_latency_ms == 0.0
        assert snap.max_queue_latency_ms == 0.0

    def test_updates_max_queue_depth(self):
        m = PipelineMetrics()
        m.observe_queue_latency(0.01, queue_depth=42)

        assert m.max_queue_depth == 42


# ── observe_processed() ─────────────────────────────────────────────────────


class TestObserveProcessed:
    def test_increments_counter(self):
        m = PipelineMetrics()
        m.observe_processed(0.01)
        m.observe_processed(0.01)
        m.observe_processed(0.01)

        assert m.processed_messages == 3

    def test_processing_time_average(self):
        m = PipelineMetrics()
        m.observe_processed(0.01)
        m.observe_processed(0.03)

        snap = m.snapshot(0)
        assert snap.average_processing_ms == pytest.approx(20.0)

    def test_processing_time_max(self):
        m = PipelineMetrics()
        m.observe_processed(0.01)
        m.observe_processed(0.05)
        m.observe_processed(0.02)

        snap = m.snapshot(0)
        assert snap.max_processing_ms == pytest.approx(50.0)

    def test_negative_clamped_to_zero(self):
        m = PipelineMetrics()
        m.observe_processed(-1.0)

        snap = m.snapshot(0)
        assert snap.average_processing_ms == 0.0
        assert snap.max_processing_ms == 0.0


# ── observe_failure() ────────────────────────────────────────────────────────


class TestObserveFailure:
    def test_increments_counter(self):
        m = PipelineMetrics()
        m.observe_failure(0.01)
        m.observe_failure(0.01)

        assert m.failed_messages == 2

    def test_contributes_to_processing_stats(self):
        m = PipelineMetrics()
        m.observe_processed(0.01)  # 10ms
        m.observe_failure(0.03)    # 30ms

        snap = m.snapshot(0)
        # Average of 10ms and 30ms = 20ms
        assert snap.average_processing_ms == pytest.approx(20.0)
        # Max is the failure's 30ms
        assert snap.max_processing_ms == pytest.approx(30.0)


# ── observe_final_row_written() ──────────────────────────────────────────────


class TestObserveFinalRowWritten:
    def test_increments_counter(self):
        m = PipelineMetrics()
        m.observe_final_row_written()
        m.observe_final_row_written()
        m.observe_final_row_written()
        m.observe_final_row_written()

        assert m.final_rows_written == 4


# ── snapshot() ───────────────────────────────────────────────────────────────


class TestSnapshot:
    def test_current_queue_depth_from_argument(self):
        m = PipelineMetrics()
        m.record_enqueue(10)

        snap = m.snapshot(7)
        assert snap.current_queue_depth == 7

    def test_after_mixed_activity(self):
        m = PipelineMetrics()

        # Enqueue 5
        for i in range(5):
            m.record_enqueue(i + 1)

        # Drop 1
        m.record_drop(6)

        # Process 3 (with varying times)
        m.observe_processed(0.01)
        m.observe_processed(0.02)
        m.observe_processed(0.03)

        # Fail 1
        m.observe_failure(0.04)

        # Write 2 final rows
        m.observe_final_row_written()
        m.observe_final_row_written()

        # Queue latency observations
        m.observe_queue_latency(0.1, queue_depth=3)
        m.observe_queue_latency(0.2, queue_depth=2)

        snap = m.snapshot(1)

        assert snap.enqueued_messages == 5
        assert snap.dropped_messages == 1
        assert snap.processed_messages == 3
        assert snap.failed_messages == 1
        assert snap.final_rows_written == 2
        assert snap.current_queue_depth == 1
        assert snap.max_queue_depth == 6
        assert snap.average_queue_latency_ms == pytest.approx(150.0)
        assert snap.max_queue_latency_ms == pytest.approx(200.0)
        # Processing: 4 observations (3 success + 1 failure) = 0.01+0.02+0.03+0.04 = 0.10 / 4 = 0.025
        assert snap.average_processing_ms == pytest.approx(25.0)
        assert snap.max_processing_ms == pytest.approx(40.0)

    def test_returns_milliseconds(self):
        m = PipelineMetrics()
        m.observe_queue_latency(1.0, queue_depth=1)   # 1 second
        m.observe_processed(0.5)                        # 0.5 seconds

        snap = m.snapshot(0)
        assert snap.average_queue_latency_ms == pytest.approx(1000.0)
        assert snap.max_queue_latency_ms == pytest.approx(1000.0)
        assert snap.average_processing_ms == pytest.approx(500.0)
        assert snap.max_processing_ms == pytest.approx(500.0)


# Need pytest for approx
import pytest
