"""Lightweight RSS memory sampler for the telemetry ingestion pipeline.

The sampler runs as a single asyncio task and periodically records the
process's resident set size (RSS).  Samples are accumulated in memory so the
pipeline can emit a peak / average / current summary on shutdown alongside
its other metrics.

Two data sources are used:
- ``/proc/self/statm`` on Linux for the current RSS (fast, no deps).
- ``resource.getrusage(RUSAGE_SELF).ru_maxrss`` for a kernel-tracked peak
  value that does not depend on our own sampling cadence catching the peak.

On non-Linux systems the statm read falls back to ``ru_maxrss`` so the
sampler still produces usable numbers.
"""

from __future__ import annotations

import asyncio
import logging
import os
import resource
import sys
from dataclasses import dataclass

LOGGER = logging.getLogger(__name__)

# On Linux ``ru_maxrss`` is reported in kilobytes; on macOS it is bytes.
# We only run production on Linux, but handle both so local dev on macOS
# doesn't report values that are ~1000x too large.
_RU_MAXRSS_SCALE_BYTES = 1024 if sys.platform.startswith("linux") else 1


@dataclass(slots=True)
class MemorySamplerSnapshot:
    """Immutable memory snapshot used for structured logging."""

    sample_count: int
    rss_current_mb: float
    rss_peak_mb: float
    rss_average_mb: float


class MemorySampler:
    """Periodically sample the process RSS and expose a snapshot."""

    def __init__(self) -> None:
        self._sample_count = 0
        self._rss_total_bytes = 0
        self._rss_peak_bytes = 0
        self._rss_current_bytes = 0
        self._page_size = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096

    def sample_once(self) -> None:
        """Take a single RSS measurement and accumulate it."""
        rss_bytes = self._read_rss_bytes()
        self._rss_current_bytes = rss_bytes
        self._rss_total_bytes += rss_bytes
        self._sample_count += 1
        if rss_bytes > self._rss_peak_bytes:
            self._rss_peak_bytes = rss_bytes

    def snapshot(self) -> MemorySamplerSnapshot:
        """Return a read-only snapshot of memory stats.

        Peak is the max of the sampled peak and the kernel-reported
        ``ru_maxrss`` so we still surface a meaningful peak even if the
        sampler was created after a short spike.
        """
        kernel_peak_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * _RU_MAXRSS_SCALE_BYTES
        peak_bytes = max(self._rss_peak_bytes, kernel_peak_bytes)

        average_bytes = 0.0
        if self._sample_count > 0:
            average_bytes = self._rss_total_bytes / self._sample_count

        return MemorySamplerSnapshot(
            sample_count=self._sample_count,
            rss_current_mb=self._rss_current_bytes / (1024 * 1024),
            rss_peak_mb=peak_bytes / (1024 * 1024),
            rss_average_mb=average_bytes / (1024 * 1024),
        )

    def _read_rss_bytes(self) -> int:
        """Read current RSS in bytes using /proc where available."""
        try:
            with open("/proc/self/statm", "r", encoding="utf-8") as handle:
                parts = handle.read().split()
            # statm format: size resident shared text lib data dt (in pages)
            resident_pages = int(parts[1])
            return resident_pages * self._page_size
        except (FileNotFoundError, IndexError, ValueError):
            # Fallback: use ru_maxrss as a rough stand-in for current RSS.
            return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * _RU_MAXRSS_SCALE_BYTES


async def memory_sampler_loop(sampler: MemorySampler, *, interval_seconds: float) -> None:
    """Sample RSS at a fixed cadence until the task is cancelled."""
    if interval_seconds <= 0:
        LOGGER.info("Memory sampler disabled (interval=%.2fs)", interval_seconds)
        return

    LOGGER.info("Memory sampler started: interval=%.2fs", interval_seconds)
    try:
        while True:
            sampler.sample_once()
            await asyncio.sleep(interval_seconds)
    except asyncio.CancelledError:
        # Take one final reading so the shutdown snapshot reflects end-of-run state.
        sampler.sample_once()
        raise
