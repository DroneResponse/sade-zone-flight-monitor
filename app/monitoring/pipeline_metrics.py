"""Lightweight runtime metrics for the local telemetry ingestion pipeline.

These metrics are intentionally simple and in-process so they are easy to use
for local scale testing. They help answer practical questions like:
- How deep did the asyncio queue get during a run?
- How long did messages wait in the queue before a worker touched them?
- How long did worker processing take per message?
- Were any messages dropped because the queue filled up?

The implementation assumes all updates happen on the asyncio event-loop thread,
which is true for this local pipeline because MQTT callback work is hopped onto
that loop before enqueueing.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class PipelineMetricsSnapshot:
    """Immutable metrics snapshot used for structured logging."""

    enqueued_messages: int
    dropped_messages: int
    processed_messages: int
    failed_messages: int
    final_rows_written: int
    current_queue_depth: int
    max_queue_depth: int
    average_queue_latency_ms: float
    max_queue_latency_ms: float
    average_processing_ms: float
    max_processing_ms: float


class PipelineMetrics:
    """Collect simple counters and latency summaries for local test runs."""

    def __init__(self) -> None:
        self.enqueued_messages = 0
        self.dropped_messages = 0
        self.processed_messages = 0
        self.failed_messages = 0
        self.final_rows_written = 0

        self.max_queue_depth = 0

        self._queue_latency_total_seconds = 0.0
        self._queue_latency_count = 0
        self._max_queue_latency_seconds = 0.0

        self._processing_total_seconds = 0.0
        self._processing_count = 0
        self._max_processing_seconds = 0.0

    def record_enqueue(self, queue_depth: int) -> None:
        """Record a successful queue insert and track the high-water mark."""
        self.enqueued_messages += 1
        self.max_queue_depth = max(self.max_queue_depth, queue_depth)

    def record_drop(self, queue_depth: int) -> None:
        """Record a message drop caused by a full queue."""
        self.dropped_messages += 1
        self.max_queue_depth = max(self.max_queue_depth, queue_depth)

    def observe_queue_latency(self, latency_seconds: float, queue_depth: int) -> None:
        """Track how long a message waited in the queue before processing."""
        safe_latency = max(0.0, latency_seconds)
        self._queue_latency_total_seconds += safe_latency
        self._queue_latency_count += 1
        self._max_queue_latency_seconds = max(self._max_queue_latency_seconds, safe_latency)
        self.max_queue_depth = max(self.max_queue_depth, queue_depth)

    def observe_processed(self, processing_seconds: float) -> None:
        """Track a successfully handled message and its worker processing time."""
        safe_duration = max(0.0, processing_seconds)
        self.processed_messages += 1
        self._processing_total_seconds += safe_duration
        self._processing_count += 1
        self._max_processing_seconds = max(self._max_processing_seconds, safe_duration)

    def observe_failure(self, processing_seconds: float) -> None:
        """Track a message-processing failure and its processing time."""
        safe_duration = max(0.0, processing_seconds)
        self.failed_messages += 1
        self._processing_total_seconds += safe_duration
        self._processing_count += 1
        self._max_processing_seconds = max(self._max_processing_seconds, safe_duration)

    def observe_final_row_written(self) -> None:
        """Track one finalized mission row write."""
        self.final_rows_written += 1

    def snapshot(self, current_queue_depth: int) -> PipelineMetricsSnapshot:
        """Return a read-only metrics snapshot for logging or reporting."""
        average_queue_latency_ms = 0.0
        if self._queue_latency_count > 0:
            average_queue_latency_ms = (self._queue_latency_total_seconds / self._queue_latency_count) * 1000.0

        average_processing_ms = 0.0
        if self._processing_count > 0:
            average_processing_ms = (self._processing_total_seconds / self._processing_count) * 1000.0

        return PipelineMetricsSnapshot(
            enqueued_messages=self.enqueued_messages,
            dropped_messages=self.dropped_messages,
            processed_messages=self.processed_messages,
            failed_messages=self.failed_messages,
            final_rows_written=self.final_rows_written,
            current_queue_depth=current_queue_depth,
            max_queue_depth=self.max_queue_depth,
            average_queue_latency_ms=average_queue_latency_ms,
            max_queue_latency_ms=self._max_queue_latency_seconds * 1000.0,
            average_processing_ms=average_processing_ms,
            max_processing_ms=self._max_processing_seconds * 1000.0,
        )
