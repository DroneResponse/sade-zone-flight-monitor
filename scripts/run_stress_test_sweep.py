#!/usr/bin/env python3
"""Run the 100-drone stress test with workers=2,4,8 and produce a comparison file.

This reuses ``scripts/run_stress_test.py`` by invoking it once per worker count
with a distinct ``--label``, so each run gets its own summary / CSV / logs.
After all runs complete, the per-run ``Shutdown metrics:`` / ``Publisher totals:``
lines are re-parsed and folded into a single side-by-side comparison file.

The scenario parameters (drone count, publish interval, runtime, queue size,
memory sample interval, etc.) are intentionally kept identical across runs so
the only variable is worker count.
"""

from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "local_test_output"
STRESS_RUNNER = REPO_ROOT / "scripts" / "run_stress_test.py"

WORKER_COUNTS = [2, 4, 8]

# Must match DRONE_COUNT in run_stress_test.py — kept here so the sweep can
# build matching filenames and expected-row counts without importing the
# runner module.
DRONE_COUNT = 100

COMPARISON_PATH = OUTPUT_DIR / f"stress_test_{DRONE_COUNT}_drones_worker_comparison.txt"


# Same regexes as run_stress_test.py — duplicated here so the comparison runner
# stays self-contained and doesn't import private helpers.
SHUTDOWN_METRICS_PATTERN = re.compile(
    r"Shutdown metrics: "
    r"queue_current=(?P<queue_current>\d+) "
    r"queue_max=(?P<queue_max>\d+) "
    r"enqueued=(?P<enqueued>\d+) "
    r"processed=(?P<processed>\d+) "
    r"failed=(?P<failed>\d+) "
    r"dropped=(?P<dropped>\d+) "
    r"final_rows=(?P<final_rows>\d+) "
    r"queue_latency_avg_ms=(?P<queue_latency_avg_ms>[0-9.]+) "
    r"queue_latency_max_ms=(?P<queue_latency_max_ms>[0-9.]+) "
    r"processing_avg_ms=(?P<processing_avg_ms>[0-9.]+) "
    r"processing_max_ms=(?P<processing_max_ms>[0-9.]+) "
    r"memory_samples=(?P<memory_samples>\d+) "
    r"rss_current_mb=(?P<rss_current_mb>[0-9.]+) "
    r"rss_peak_mb=(?P<rss_peak_mb>[0-9.]+) "
    r"rss_avg_mb=(?P<rss_avg_mb>[0-9.]+)"
)

PUBLISHER_TOTALS_PATTERN = re.compile(
    r"Publisher totals: "
    r"published=(?P<published>\d+) "
    r"drones=(?P<drones>\d+) "
    r"avg_per_drone=(?P<avg_per_drone>[0-9.]+)"
)


@dataclass(slots=True)
class SweepRun:
    """Parsed result of one entry in the worker-count sweep."""

    worker_count: int
    label: str
    summary_path: Path
    csv_path: Path
    runner_log_path: Path

    # Ingestion pipeline
    queue_max: int
    enqueued: int
    processed: int
    dropped: int
    final_rows: int
    queue_latency_avg_ms: float
    queue_latency_max_ms: float
    processing_avg_ms: float
    processing_max_ms: float

    # Memory
    memory_samples: int
    rss_current_mb: float
    rss_peak_mb: float
    rss_avg_mb: float

    # Publisher-side
    published_total: int


def output_paths_for(worker_count: int) -> tuple[str, Path, Path, Path, Path]:
    """Return (label, summary, csv, runner_log, mosquitto_log) for a worker count."""
    label = f"{worker_count}workers"
    prefix = OUTPUT_DIR / f"stress_test_{DRONE_COUNT}_drones_{label}"
    return (
        label,
        prefix.with_name(prefix.name + "_summary.txt"),
        prefix.with_name(prefix.name + "_rows.csv"),
        prefix.with_name(prefix.name + "_runner.log"),
        prefix.with_name(prefix.name + "_mosquitto.log"),
    )


def run_one(worker_count: int) -> SweepRun:
    """Invoke run_stress_test.py for a single worker count and parse its log."""
    label, summary_path, csv_path, runner_log_path, _mosquitto_log = output_paths_for(worker_count)

    print(f"\n=== Running stress test: workers={worker_count} (label={label}) ===")
    subprocess.run(
        [sys.executable, str(STRESS_RUNNER), "--workers", str(worker_count), "--label", label],
        check=True,
        cwd=REPO_ROOT,
    )

    log_text = runner_log_path.read_text(encoding="utf-8")
    shutdown_match = list(SHUTDOWN_METRICS_PATTERN.finditer(log_text))
    publisher_match = list(PUBLISHER_TOTALS_PATTERN.finditer(log_text))
    if not shutdown_match or not publisher_match:
        raise RuntimeError(
            f"Could not parse required log lines from {runner_log_path} for workers={worker_count}"
        )

    shutdown = shutdown_match[-1].groupdict()
    publisher = publisher_match[-1].groupdict()

    return SweepRun(
        worker_count=worker_count,
        label=label,
        summary_path=summary_path,
        csv_path=csv_path,
        runner_log_path=runner_log_path,
        queue_max=int(shutdown["queue_max"]),
        enqueued=int(shutdown["enqueued"]),
        processed=int(shutdown["processed"]),
        dropped=int(shutdown["dropped"]),
        final_rows=int(shutdown["final_rows"]),
        queue_latency_avg_ms=float(shutdown["queue_latency_avg_ms"]),
        queue_latency_max_ms=float(shutdown["queue_latency_max_ms"]),
        processing_avg_ms=float(shutdown["processing_avg_ms"]),
        processing_max_ms=float(shutdown["processing_max_ms"]),
        memory_samples=int(shutdown["memory_samples"]),
        rss_current_mb=float(shutdown["rss_current_mb"]),
        rss_peak_mb=float(shutdown["rss_peak_mb"]),
        rss_avg_mb=float(shutdown["rss_avg_mb"]),
        published_total=int(publisher["published"]),
    )


def _row(cells: list[str], widths: list[int]) -> str:
    """Format a single table row given cells and per-column widths."""
    padded = [cell.ljust(width) for cell, width in zip(cells, widths)]
    return "| " + " | ".join(padded) + " |"


def _sep(widths: list[int]) -> str:
    """Produce the divider row for a table with the given column widths."""
    return "|" + "|".join("-" * (width + 2) for width in widths) + "|"


def write_comparison(runs: list[SweepRun], comparison_path: Path) -> None:
    """Write a markdown-ish side-by-side comparison across the sweep runs."""
    headers = ["Metric"] + [f"{run.worker_count} workers" for run in runs]

    def fmt_int(value: int) -> str:
        return f"{value:,}"

    def fmt_float(value: float, precision: int = 2) -> str:
        return f"{value:,.{precision}f}"

    rows: list[list[str]] = [
        [f"Final rows (expected {DRONE_COUNT})"] + [fmt_int(r.final_rows) for r in runs],
        ["Enqueued messages"] + [fmt_int(r.enqueued) for r in runs],
        ["Processed messages"] + [fmt_int(r.processed) for r in runs],
        ["Publisher total (published)"] + [fmt_int(r.published_total) for r in runs],
        ["Ingestion drops"] + [fmt_int(r.dropped) for r in runs],
        ["Broker-side drop estimate"] + [fmt_int(max(0, r.published_total - r.enqueued)) for r in runs],
        ["Max queue depth"] + [fmt_int(r.queue_max) for r in runs],
        ["Queue latency avg (ms)"] + [fmt_float(r.queue_latency_avg_ms) for r in runs],
        ["Queue latency max (ms)"] + [fmt_float(r.queue_latency_max_ms) for r in runs],
        ["Processing avg (ms)"] + [fmt_float(r.processing_avg_ms) for r in runs],
        ["Processing max (ms)"] + [fmt_float(r.processing_max_ms) for r in runs],
        ["RSS peak (MB)"] + [fmt_float(r.rss_peak_mb) for r in runs],
        ["RSS average (MB)"] + [fmt_float(r.rss_avg_mb) for r in runs],
        ["RSS at exit (MB)"] + [fmt_float(r.rss_current_mb) for r in runs],
        ["Memory samples"] + [fmt_int(r.memory_samples) for r in runs],
    ]

    widths = [max(len(headers[col]), *(len(row[col]) for row in rows)) for col in range(len(headers))]

    lines: list[str] = []
    lines.append(f"{DRONE_COUNT}-Drone Stress Test — Worker Count Comparison")
    lines.append("=" * len(lines[-1]))
    lines.append("")
    lines.append("Scenario (identical across all runs)")
    lines.append("------------------------------------")
    lines.append(
        f"{DRONE_COUNT} drones, 0.1s publish interval (10 msg/s per drone), groups of 15 staggered "
        "10s apart, 300s per-drone runtime, queue size 30000, RSS sampled every 2s."
    )
    lines.append("Only the worker count varies.")
    lines.append("")
    lines.append("Side-by-side metrics")
    lines.append("--------------------")
    lines.append(_row(headers, widths))
    lines.append(_sep(widths))
    for row in rows:
        lines.append(_row(row, widths))
    lines.append("")

    # Memory-focused block in case the user wants to paste a clean snippet.
    lines.append("Memory snapshot (for sharing)")
    lines.append("-----------------------------")
    for run in runs:
        lines.append(
            f"- {run.worker_count} workers:  peak={run.rss_peak_mb:.2f} MB,  "
            f"avg={run.rss_avg_mb:.2f} MB,  at_exit={run.rss_current_mb:.2f} MB,  "
            f"samples={run.memory_samples}"
        )
    lines.append("")

    lines.append("Per-run artifacts")
    lines.append("-----------------")
    for run in runs:
        lines.append(f"{run.worker_count} workers:")
        lines.append(f"  summary:    {run.summary_path}")
        lines.append(f"  CSV:        {run.csv_path}")
        lines.append(f"  runner log: {run.runner_log_path}")
    lines.append("")

    comparison_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    """Run the sweep and write the comparison file."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    runs = [run_one(worker_count) for worker_count in WORKER_COUNTS]
    write_comparison(runs, COMPARISON_PATH)

    print()
    print(COMPARISON_PATH.read_text(encoding="utf-8"))
    print(f"Comparison written to {COMPARISON_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
