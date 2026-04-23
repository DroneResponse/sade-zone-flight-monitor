#!/usr/bin/env python3
"""Run a 100-drone stress test against the telemetry ingestion pipeline.

Scenario:
- 100 simulated drones publishing telemetry every 0.1s (1000 msg/s sustained).
- Drones start in groups of 15, staggered 10s apart (~60s total ramp).
- Each drone runs for 300s, so the full scenario covers ~360s of sustained load.
- Pipeline configured with 2 workers (by default) and a 30000-slot queue.
- RSS is sampled every 2s inside the pipeline process.

Outputs (under ``local_test_output/``):
- ``stress_test_100_drones_summary.txt``  — human-readable summary
- ``stress_test_100_drones_rows.csv``     — one row per finalized drone mission
- ``stress_test_100_drones_runner.log``   — full runner + pipeline + API logs
- ``stress_test_100_drones_mosquitto.log``— broker log

The summary includes:
- Throughput (enqueued/s, processed/s)
- Drop rates (ingestion-side, broker-side estimate from publish vs enqueue counts)
- Queue depth and queue/processing latency
- RSS peak / average / current
- CSV verification (row count, unique uav_ids, populated-telemetry count)
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "local_test_output"
INTEGRATION_RUNNER = REPO_ROOT / "tests" / "integration" / "test_mqtt_telemetry_pipeline.py"

# ── Scenario parameters ───────────────────────────────────────────────────────
DRONE_COUNT = 100
PUBLISH_INTERVAL_SECONDS = 0.1
GROUP_SIZE = 15
GROUP_STAGGER_SECONDS = 10.0
PUBLISHER_RUNTIME_SECONDS = 300.0
DEFAULT_WORKER_COUNT = 2
QUEUE_SIZE = 30000
METRICS_LOG_INTERVAL = 10.0
MEMORY_SAMPLE_INTERVAL = 2.0


def build_output_paths(label: str) -> tuple[Path, Path, Path, Path]:
    """Return (summary, csv, runner_log, mosquitto_log) paths for a run label."""
    prefix = OUTPUT_DIR / f"stress_test_{DRONE_COUNT}_drones_{label}"
    return (
        prefix.with_name(prefix.name + "_summary.txt"),
        prefix.with_name(prefix.name + "_rows.csv"),
        prefix.with_name(prefix.name + "_runner.log"),
        prefix.with_name(prefix.name + "_mosquitto.log"),
    )

# ── Log parsing ───────────────────────────────────────────────────────────────
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
class StressTestResult:
    """Parsed result of one stress test run."""

    duration_seconds: float

    # Ingestion pipeline metrics
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

    # Memory metrics
    memory_samples: int
    rss_current_mb: float
    rss_peak_mb: float
    rss_avg_mb: float

    # Publisher-side metrics
    published_total: int
    publisher_drones: int
    avg_published_per_drone: float

    # CSV verification
    csv_rows: int
    csv_unique_uav_ids: int
    csv_rows_with_altitude: int
    csv_rows_with_voltage: int


def build_command(
    *,
    worker_count: int,
    csv_path: Path,
    runner_log_path: Path,
    mosquitto_log_path: Path,
) -> list[str]:
    """Build the subprocess command for the 100-drone scenario."""
    return [
        sys.executable,
        str(INTEGRATION_RUNNER),
        "--drone-count", str(DRONE_COUNT),
        "--publish-interval", str(PUBLISH_INTERVAL_SECONDS),
        "--group-size", str(GROUP_SIZE),
        "--group-stagger-seconds", str(GROUP_STAGGER_SECONDS),
        "--publisher-runtime-seconds", str(PUBLISHER_RUNTIME_SECONDS),
        "--workers", str(worker_count),
        "--queue-size", str(QUEUE_SIZE),
        "--metrics-log-interval", str(METRICS_LOG_INTERVAL),
        "--memory-sample-interval", str(MEMORY_SAMPLE_INTERVAL),
        "--output-csv", str(csv_path),
        "--runner-log", str(runner_log_path),
        "--mosquitto-log", str(mosquitto_log_path),
    ]


def parse_shutdown_metrics(runner_log_path: Path) -> dict[str, str]:
    """Extract the final ``Shutdown metrics:`` line from the runner log."""
    text = runner_log_path.read_text(encoding="utf-8")
    matches = list(SHUTDOWN_METRICS_PATTERN.finditer(text))
    if not matches:
        raise RuntimeError(
            f"No shutdown metrics line found in {runner_log_path}. "
            "Did the pipeline shut down cleanly?"
        )
    return matches[-1].groupdict()


def parse_publisher_totals(runner_log_path: Path) -> dict[str, str]:
    """Extract the ``Publisher totals:`` line from the runner log."""
    text = runner_log_path.read_text(encoding="utf-8")
    matches = list(PUBLISHER_TOTALS_PATTERN.finditer(text))
    if not matches:
        raise RuntimeError(
            f"No publisher totals line found in {runner_log_path}. "
            "Did the drone publishers finish?"
        )
    return matches[-1].groupdict()


def verify_csv(csv_path: Path) -> tuple[int, int, int, int]:
    """Read the finalized mission CSV and return verification counts.

    Returns ``(row_count, unique_uav_ids, rows_with_altitude, rows_with_voltage)``.
    """
    if not csv_path.exists():
        return 0, 0, 0, 0

    with csv_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    unique_uav_ids = {row.get("uav_id") for row in rows if row.get("uav_id")}

    def _is_nonzero_numeric(value: str | None) -> bool:
        if not value:
            return False
        try:
            return float(value) != 0.0
        except ValueError:
            # Columns like ``battery.voltage_in`` are JSON-encoded dicts; treat
            # any non-empty string that isn't a plain "0" as populated.
            return value not in {"", "{}", "0", "0.0"}

    rows_with_altitude = sum(
        1 for row in rows if _is_nonzero_numeric(row.get("flight.max_alt_asl_m"))
    )
    rows_with_voltage = sum(
        1 for row in rows if _is_nonzero_numeric(row.get("battery.voltage_in"))
    )

    return len(rows), len(unique_uav_ids), rows_with_altitude, rows_with_voltage


def run_stress_test(
    *,
    worker_count: int,
    csv_path: Path,
    runner_log_path: Path,
    mosquitto_log_path: Path,
) -> StressTestResult:
    """Run the 100-drone stress scenario and parse results from its logs."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Fresh outputs each run so summaries are never computed from stale data.
    for path in (csv_path, runner_log_path, mosquitto_log_path):
        if path.exists():
            path.unlink()

    command = build_command(
        worker_count=worker_count,
        csv_path=csv_path,
        runner_log_path=runner_log_path,
        mosquitto_log_path=mosquitto_log_path,
    )
    print(f"Launching stress test subprocess:\n  {' '.join(command)}")

    started_at = time.monotonic()
    subprocess.run(command, check=True, cwd=REPO_ROOT)
    duration_seconds = time.monotonic() - started_at

    shutdown = parse_shutdown_metrics(runner_log_path)
    publisher = parse_publisher_totals(runner_log_path)
    csv_rows, unique_uav_ids, with_altitude, with_voltage = verify_csv(csv_path)

    return StressTestResult(
        duration_seconds=duration_seconds,
        queue_current=int(shutdown["queue_current"]),
        queue_max=int(shutdown["queue_max"]),
        enqueued=int(shutdown["enqueued"]),
        processed=int(shutdown["processed"]),
        failed=int(shutdown["failed"]),
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
        publisher_drones=int(publisher["drones"]),
        avg_published_per_drone=float(publisher["avg_per_drone"]),
        csv_rows=csv_rows,
        csv_unique_uav_ids=unique_uav_ids,
        csv_rows_with_altitude=with_altitude,
        csv_rows_with_voltage=with_voltage,
    )


def _pct(numerator: float, denominator: float) -> str:
    """Format a ratio as a percentage string; safe on zero denominator."""
    if denominator <= 0:
        return "n/a"
    return f"{(numerator / denominator) * 100.0:.2f}%"


def _rate(count: int, duration_seconds: float) -> str:
    """Format a per-second rate; safe on zero duration."""
    if duration_seconds <= 0:
        return "n/a"
    return f"{count / duration_seconds:.1f} msg/s"


def write_summary(
    result: StressTestResult,
    summary_path: Path,
    *,
    worker_count: int,
    csv_path: Path,
    runner_log_path: Path,
    mosquitto_log_path: Path,
) -> None:
    """Write the stress test summary to a plain-text file."""
    broker_drop = max(0, result.published_total - result.enqueued)
    ingestion_drop_rate = _pct(result.dropped, result.enqueued)
    broker_drop_rate = _pct(broker_drop, result.published_total)

    lines: list[str] = []
    lines.append(f"{DRONE_COUNT}-Drone Stress Test Summary")
    lines.append("============================")
    lines.append("")
    lines.append("Scenario")
    lines.append("--------")
    lines.append(
        f"{DRONE_COUNT} drones, publish interval {PUBLISH_INTERVAL_SECONDS}s, "
        f"groups of {GROUP_SIZE} staggered {GROUP_STAGGER_SECONDS}s apart, "
        f"{PUBLISHER_RUNTIME_SECONDS}s runtime per drone, "
        f"{worker_count} workers, queue size {QUEUE_SIZE}."
    )
    lines.append(
        f"Total wall-clock duration: {result.duration_seconds:.1f}s "
        f"(expected ~{((DRONE_COUNT - 1) // GROUP_SIZE) * GROUP_STAGGER_SECONDS + PUBLISHER_RUNTIME_SECONDS:.0f}s)."
    )
    lines.append("")

    lines.append("Throughput")
    lines.append("----------")
    lines.append(f"Enqueued:  {result.enqueued} messages  ({_rate(result.enqueued, result.duration_seconds)})")
    lines.append(f"Processed: {result.processed} messages  ({_rate(result.processed, result.duration_seconds)})")
    lines.append(f"Failed:    {result.failed} messages")
    lines.append(f"Publisher totals: {result.published_total} messages across {result.publisher_drones} drones "
                 f"(avg {result.avg_published_per_drone:.1f} per drone)")
    lines.append("")

    lines.append("Drops")
    lines.append("-----")
    lines.append(f"Ingestion-side drops (full queue, bad payload, unregistered session): "
                 f"{result.dropped} / {result.enqueued}  [{ingestion_drop_rate}]")
    lines.append(f"Broker-side / network loss estimate (published - enqueued): "
                 f"{broker_drop} / {result.published_total}  [{broker_drop_rate}]")
    lines.append("")

    lines.append("Latency")
    lines.append("-------")
    lines.append(f"Queue latency:      avg={result.queue_latency_avg_ms:.2f} ms, "
                 f"max={result.queue_latency_max_ms:.2f} ms")
    lines.append(f"Processing latency: avg={result.processing_avg_ms:.2f} ms, "
                 f"max={result.processing_max_ms:.2f} ms")
    lines.append(f"Queue depth:        max={result.queue_max}  (current at shutdown={result.queue_current})")
    lines.append("")

    lines.append("Memory (pipeline process RSS)")
    lines.append("-----------------------------")
    if result.memory_samples > 0:
        lines.append(f"Samples:  {result.memory_samples} at {MEMORY_SAMPLE_INTERVAL}s interval")
        lines.append(f"Peak:     {result.rss_peak_mb:.2f} MB")
        lines.append(f"Average:  {result.rss_avg_mb:.2f} MB")
        lines.append(f"At exit:  {result.rss_current_mb:.2f} MB")
    else:
        lines.append("Memory sampling was disabled (no samples recorded).")
    lines.append("")

    lines.append("CSV verification")
    lines.append("----------------")
    lines.append(f"CSV path: {csv_path}")
    lines.append(f"Rows written:                {result.csv_rows}  (expected {DRONE_COUNT})")
    lines.append(f"Unique uav_id values:        {result.csv_unique_uav_ids}  (expected {DRONE_COUNT})")
    lines.append(f"Rows with max altitude > 0:  {result.csv_rows_with_altitude}")
    lines.append(f"Rows with voltage_in set:    {result.csv_rows_with_voltage}")

    rows_match = result.csv_rows == DRONE_COUNT and result.csv_unique_uav_ids == DRONE_COUNT
    telemetry_populated = (
        result.csv_rows_with_altitude == result.csv_rows
        and result.csv_rows_with_voltage == result.csv_rows
        and result.csv_rows > 0
    )
    lines.append("")
    if rows_match and telemetry_populated:
        lines.append("Result: PASS — one row per drone, all telemetry fields populated.")
    elif rows_match:
        lines.append("Result: PARTIAL — row count matches drone count, but some rows have "
                     "missing altitude or voltage data. Inspect the CSV.")
    else:
        lines.append("Result: FAIL — row count or unique uav_id count does not match drone count. "
                     "Inspect the runner log for drones that never finalized.")
    lines.append("")

    lines.append("Artifacts")
    lines.append("---------")
    lines.append(f"CSV:            {csv_path}")
    lines.append(f"Runner log:     {runner_log_path}")
    lines.append(f"Mosquitto log:  {mosquitto_log_path}")
    lines.append("")

    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    """Run the stress test and write the summary."""
    parser = argparse.ArgumentParser(description=f"Run a {DRONE_COUNT}-drone stress test scenario")
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKER_COUNT,
        help=f"Number of ingestion worker tasks (default {DEFAULT_WORKER_COUNT})",
    )
    parser.add_argument(
        "--label",
        default=None,
        help=(
            "Suffix for output filenames. Defaults to '{N}workers' so sweeps "
            "write to stress_test_<drone_count>_drones_{label}_*.{csv,txt,log}."
        ),
    )
    args = parser.parse_args()

    label = args.label or f"{args.workers}workers"
    summary_path, csv_path, runner_log_path, mosquitto_log_path = build_output_paths(label)

    result = run_stress_test(
        worker_count=args.workers,
        csv_path=csv_path,
        runner_log_path=runner_log_path,
        mosquitto_log_path=mosquitto_log_path,
    )
    write_summary(
        result,
        summary_path,
        worker_count=args.workers,
        csv_path=csv_path,
        runner_log_path=runner_log_path,
        mosquitto_log_path=mosquitto_log_path,
    )

    print()
    print(summary_path.read_text(encoding="utf-8"))
    print(f"Summary written to {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
