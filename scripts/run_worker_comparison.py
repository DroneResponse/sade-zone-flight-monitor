#!/usr/bin/env python3
"""Run a repeatable local worker-scaling comparison for the MQTT test harness.

This script is intentionally small and practical for local testing:
- It reuses ``run_local_test.py`` for the actual end-to-end workload.
- It runs the same 30-drone scenario for multiple worker counts.
- It parses the shutdown metrics already written by the ingestion pipeline.
- It writes a short plain-text summary file when all runs are complete.

The goal is to make worker-count comparisons easy to repeat without hand
collecting CSV counts and metric lines from multiple log files.
"""

from __future__ import annotations

import csv
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "local_test_output"
RUN_LOCAL_TEST = REPO_ROOT / "tests" / "integration" / "test_mqtt_telemetry_pipeline.py"
SUMMARY_PATH = OUTPUT_DIR / "worker_comparison_summary.txt"

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
    r"processing_max_ms=(?P<processing_max_ms>[0-9.]+)"
)


@dataclass(slots=True)
class ScenarioResult:
    """Summary of one worker-count comparison run."""

    workers: int
    duration_seconds: float
    csv_rows: int
    unique_uav_ids: int
    queue_current: int
    queue_max: int
    enqueued: int
    processed: int
    failed: int
    dropped: int
    final_rows: int
    queue_latency_avg_ms: float
    queue_latency_max_ms: float
    processing_avg_ms: float
    processing_max_ms: float
    csv_path: Path
    runner_log_path: Path
    mosquitto_log_path: Path


def build_command(*, workers: int, metrics_log_interval: float) -> tuple[list[str], Path, Path, Path]:
    """Build the local test command and output paths for one worker scenario."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    prefix = OUTPUT_DIR / f"worker_comparison_{workers}_workers"
    csv_path = prefix.with_name(prefix.name + "_rows.csv")
    runner_log_path = prefix.with_name(prefix.name + "_runner.log")
    mosquitto_log_path = prefix.with_name(prefix.name + "_mosquitto.log")

    command = [
        sys.executable,
        str(RUN_LOCAL_TEST),
        "--drone-count",
        "30",
        "--group-size",
        "5",
        "--group-stagger-seconds",
        "60",
        "--publisher-runtime-seconds",
        "300",
        "--publish-interval",
        "0.1",
        "--workers",
        str(workers),
        "--metrics-log-interval",
        str(metrics_log_interval),
        "--output-csv",
        str(csv_path),
        "--runner-log",
        str(runner_log_path),
        "--mosquitto-log",
        str(mosquitto_log_path),
    ]
    return command, csv_path, runner_log_path, mosquitto_log_path


def parse_shutdown_metrics(runner_log_path: Path) -> dict[str, str]:
    """Extract the final shutdown metrics block from a runner log file."""
    text = runner_log_path.read_text(encoding="utf-8")
    matches = list(SHUTDOWN_METRICS_PATTERN.finditer(text))
    if not matches:
        raise RuntimeError(f"No shutdown metrics found in {runner_log_path}")
    return matches[-1].groupdict()


def count_csv_rows(csv_path: Path) -> tuple[int, int]:
    """Count finalized mission rows and distinct UAV ids from one output CSV."""
    with csv_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    unique_uav_ids = {row.get("uav_id") for row in rows if row.get("uav_id")}
    return len(rows), len(unique_uav_ids)


def run_scenario(*, workers: int, metrics_log_interval: float) -> ScenarioResult:
    """Run one local scale scenario and return the parsed result summary."""
    command, csv_path, runner_log_path, mosquitto_log_path = build_command(
        workers=workers,
        metrics_log_interval=metrics_log_interval,
    )

    started_at = time.monotonic()
    subprocess.run(command, check=True, cwd=REPO_ROOT)
    duration_seconds = time.monotonic() - started_at

    shutdown_metrics = parse_shutdown_metrics(runner_log_path)
    csv_rows, unique_uav_ids = count_csv_rows(csv_path)

    return ScenarioResult(
        workers=workers,
        duration_seconds=duration_seconds,
        csv_rows=csv_rows,
        unique_uav_ids=unique_uav_ids,
        queue_current=int(shutdown_metrics["queue_current"]),
        queue_max=int(shutdown_metrics["queue_max"]),
        enqueued=int(shutdown_metrics["enqueued"]),
        processed=int(shutdown_metrics["processed"]),
        failed=int(shutdown_metrics["failed"]),
        dropped=int(shutdown_metrics["dropped"]),
        final_rows=int(shutdown_metrics["final_rows"]),
        queue_latency_avg_ms=float(shutdown_metrics["queue_latency_avg_ms"]),
        queue_latency_max_ms=float(shutdown_metrics["queue_latency_max_ms"]),
        processing_avg_ms=float(shutdown_metrics["processing_avg_ms"]),
        processing_max_ms=float(shutdown_metrics["processing_max_ms"]),
        csv_path=csv_path,
        runner_log_path=runner_log_path,
        mosquitto_log_path=mosquitto_log_path,
    )


def choose_best_results(results: Iterable[ScenarioResult]) -> tuple[ScenarioResult, ScenarioResult, ScenarioResult]:
    """Pick simple winners for queue depth, queue latency, and processing time."""
    results = list(results)
    best_queue_depth = min(results, key=lambda item: item.queue_max)
    best_queue_latency = min(results, key=lambda item: item.queue_latency_avg_ms)
    best_processing = min(results, key=lambda item: item.processing_avg_ms)
    return best_queue_depth, best_queue_latency, best_processing


def write_summary(results: list[ScenarioResult], summary_path: Path) -> None:
    """Write a short human-readable comparison summary to a text file."""
    best_queue_depth, best_queue_latency, best_processing = choose_best_results(results)

    lines: list[str] = []
    lines.append("Local Worker Comparison Summary")
    lines.append("===============================")
    lines.append("")
    lines.append("Scenario")
    lines.append("--------")
    lines.append("30 drones, publish interval 0.1s, groups of 5, 60s stagger, 300s runtime per drone.")
    lines.append("")
    lines.append("Results")
    lines.append("-------")
    for result in results:
        lines.append(
            f"{result.workers} worker(s): rows={result.csv_rows}, unique_uav_ids={result.unique_uav_ids}, "
            f"final_rows={result.final_rows}, dropped={result.dropped}, queue_max={result.queue_max}, "
            f"queue_latency_avg_ms={result.queue_latency_avg_ms:.2f}, queue_latency_max_ms={result.queue_latency_max_ms:.2f}, "
            f"processing_avg_ms={result.processing_avg_ms:.2f}, processing_max_ms={result.processing_max_ms:.2f}, "
            f"duration_seconds={result.duration_seconds:.1f}"
        )
    lines.append("")
    lines.append("Quick Take")
    lines.append("----------")
    lines.append(f"Lowest max queue depth: {best_queue_depth.workers} worker(s) with queue_max={best_queue_depth.queue_max}.")
    lines.append(f"Lowest average queue latency: {best_queue_latency.workers} worker(s) with queue_latency_avg_ms={best_queue_latency.queue_latency_avg_ms:.2f}.")
    lines.append(f"Lowest average processing time: {best_processing.workers} worker(s) with processing_avg_ms={best_processing.processing_avg_ms:.2f}.")
    lines.append("")
    lines.append("Artifacts")
    lines.append("---------")
    for result in results:
        lines.append(f"{result.workers} worker(s) CSV: {result.csv_path}")
        lines.append(f"{result.workers} worker(s) runner log: {result.runner_log_path}")
        lines.append(f"{result.workers} worker(s) broker log: {result.mosquitto_log_path}")
    lines.append("")
    lines.append("Notes")
    lines.append("-----")
    lines.append("All metrics come from the ingestion pipeline shutdown summary and the finalized CSV output files.")

    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    """Run the worker comparison scenarios and write the summary file."""
    worker_counts = [1, 2, 4]
    metrics_log_interval = 30.0

    results: list[ScenarioResult] = []
    for workers in worker_counts:
        print(f"Running scenario for workers={workers}...")
        result = run_scenario(workers=workers, metrics_log_interval=metrics_log_interval)
        results.append(result)
        print(
            f"Completed workers={workers}: rows={result.csv_rows} "
            f"queue_max={result.queue_max} queue_latency_avg_ms={result.queue_latency_avg_ms:.2f}"
        )

    write_summary(results, SUMMARY_PATH)
    print(f"Summary written to {SUMMARY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
