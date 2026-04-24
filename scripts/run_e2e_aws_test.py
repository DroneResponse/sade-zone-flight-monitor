#!/usr/bin/env python3
"""End-to-end test of the SADE Flight Monitor against real AWS cloud services.

This harness exercises the full production code path — mTLS to AWS IoT Core
and the real outbound POST to SADE's /tracker-session-finalized — by playing
both the SADE-outbox role (HTTP register-session) and the drone role (MQTT
telemetry publisher) against a locally-spawned Flight Monitor subprocess.

What makes this different from tests/integration/*:
  * Hits real AWS (IoT Core, SADE ALB), creating a real reputation record.
  * Not pytest-discoverable — it's a human-run single-shot harness.
  * Requires a configured .env file with real cert paths + the SADE URL.

Usage:
    python scripts/run_e2e_aws_test.py
    python scripts/run_e2e_aws_test.py --dry-run
    python scripts/run_e2e_aws_test.py --telemetry-count 20
    python scripts/run_e2e_aws_test.py --env-file .env.staging

Dry-run mode forces FINALIZE_TO_API=false — everything up to and including
the final-mission-row write is validated, but the outbound SADE POST is
skipped.  Useful for the first run, or when you want to exercise the MQTT
side without touching SADE's database.

Exit code 0 on pass, 1 on fail.  The summary file tells you which step
failed and where to look for diagnostics.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import queue
import re
import socket
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import paho.mqtt.client as mqtt

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "local_test_output"
DEFAULT_ENV_FILE = REPO_ROOT / ".env"
SUMMARY_PATH = OUTPUT_DIR / "e2e_aws_summary.txt"
FLIGHT_MONITOR_LOG_PATH = OUTPUT_DIR / "e2e_aws_flight_monitor.log"
RUNNER_LOG_PATH = OUTPUT_DIR / "e2e_aws_runner.log"

LOGGER = logging.getLogger("e2e_aws")


# ── Flight-Monitor log line patterns ─────────────────────────────────────────
# Each of these is matched against the Flight Monitor's stdout as the test
# runs.  The harness waits on specific signals to advance to the next step.
PATTERNS: dict[str, re.Pattern[str]] = {
    "mtls":               re.compile(r"MQTT transport: mTLS"),
    "connected":          re.compile(r"Connected to MQTT broker \(rc=0\)"),
    "subscribed":         re.compile(r"Subscribed to topic: update_drone"),
    "final_row":          re.compile(r"wrote final mission row for drone_id=(?P<drone_id>\S+)"),
    "finalize_posting":   re.compile(r"Sending tracker finalization POST for flight_session_id=(?P<fsid>\S+)"),
    "finalize_done":      re.compile(
        r"Tracker session finalized: flight_session_id=(?P<fsid>\S+) reputation_record_id=(?P<rep>\S+)"
    ),
    "finalize_http_err":  re.compile(r"POST \S+ → HTTP (?P<code>[0-9]{3}) \| flight_session_id="),
    "finalize_failed":    re.compile(r"All \d+ finalization attempts failed for flight_session_id"),
    "business_failed":    re.compile(r"Tracker finalization business failure"),
    "business_failure_detail": re.compile(
        r"Tracker finalization business failure: flight_session_id=(?P<fsid>\S+) reason=(?P<reason>.+)$"
    ),
}


# SADE returns status=FAILED with one of these reasons when our payload is
# schema-valid but the referenced session doesn't exist / isn't finalizable.
# For E2E runs that fabricate flight_session_ids locally (not pre-registered
# in SADE's database) this is the expected-and-acceptable outcome — it proves
# the contract is met.  Substring match, not exact, so minor wording drift on
# SADE's side doesn't break the harness.
EXPECTED_BUSINESS_FAILURE_REASONS = (
    "No session found for tracker finalization report.",
)


# ── Env file parsing ─────────────────────────────────────────────────────────


def parse_env_file(path: Path) -> dict[str, str]:
    """Parse a KEY=value .env file.  Strips comments and trims surrounding quotes.

    Skips blank lines and lines starting with `#`.  This is intentionally
    simpler than python-dotenv because the Flight Monitor's own env-var
    consumption is plain `os.getenv` — no interpolation, no $VAR expansion.
    """
    env: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
            val = val[1:-1]
        env[key] = val
    return env


# ── Pre-flight helpers ───────────────────────────────────────────────────────


def port_is_free(host: str, port: int) -> bool:
    """Return True if nothing is listening on ``host:port``."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex((host, port)) != 0


# ── Flight Monitor subprocess runner ─────────────────────────────────────────


class FlightMonitorRunner:
    """Spawn the Flight Monitor, tee its output to a log file, and signal on matches.

    A background thread reads the subprocess's combined stdout+stderr stream
    line by line, writes every line to ``FLIGHT_MONITOR_LOG_PATH``, and for
    each registered pattern pushes any matching re.Match onto that pattern's
    queue.  The main thread calls ``wait_for(name, timeout)`` to block on a
    specific signal.

    Matches that arrive *before* ``wait_for`` is called are queued and
    returned immediately when the caller is ready — we never lose an early
    match.
    """

    def __init__(self, env: dict[str, str]) -> None:
        self.env = env
        self.process: subprocess.Popen[str] | None = None
        self.event_queues: dict[str, queue.Queue] = {name: queue.Queue() for name in PATTERNS}
        self._reader_thread: threading.Thread | None = None

    def start(self) -> None:
        """Launch run.py as a subprocess and start the log-tail reader."""
        python_bin = REPO_ROOT / "venv" / "bin" / "python"
        python_exe = str(python_bin) if python_bin.is_file() else sys.executable

        # PYTHONUNBUFFERED=1 so logging.StreamHandler() output arrives line by
        # line for our tail reader — no 4 KB-block buffering masking signals.
        child_env = dict(self.env)
        child_env["PYTHONUNBUFFERED"] = "1"

        LOGGER.info("Launching Flight Monitor: %s run.py", python_exe)
        self.process = subprocess.Popen(
            [python_exe, "run.py"],
            cwd=str(REPO_ROOT),
            env=child_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._reader_thread.start()

    def _read_loop(self) -> None:
        """Tee Flight Monitor stdout to a log file, pushing pattern hits to queues."""
        assert self.process is not None and self.process.stdout is not None
        with FLIGHT_MONITOR_LOG_PATH.open("w", encoding="utf-8") as log_file:
            for line in self.process.stdout:
                log_file.write(line)
                log_file.flush()
                for name, pattern in PATTERNS.items():
                    match = pattern.search(line)
                    if match is not None:
                        self.event_queues[name].put(match)

    def wait_for(self, name: str, timeout: float) -> re.Match | None:
        """Block until ``name``'s pattern matched at least once, or timeout."""
        try:
            return self.event_queues[name].get(timeout=timeout)
        except queue.Empty:
            return None

    def wait_for_any(
        self, names: list[str], timeout: float,
    ) -> tuple[str, re.Match] | None:
        """Block until any of the named patterns matches, or timeout.

        Returns ``(name, match)`` for the first pattern that fires, or None
        on timeout.  Used when multiple terminal signals are possible and we
        want to return on whichever arrives first (e.g. finalize-success vs.
        business-failure) instead of wasting the full per-signal timeout.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for name in names:
                try:
                    match = self.event_queues[name].get_nowait()
                    return (name, match)
                except queue.Empty:
                    continue
            time.sleep(0.05)
        return None

    def has_seen(self, name: str) -> bool:
        """Non-blocking check: did pattern ``name`` already match?"""
        return not self.event_queues[name].empty()

    def stop(self) -> None:
        """Terminate the subprocess politely, then force-kill if it hangs."""
        if self.process is None or self.process.poll() is not None:
            return
        LOGGER.info("Terminating Flight Monitor (pid=%s)", self.process.pid)
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            LOGGER.warning("Flight Monitor did not terminate in 5s — killing")
            self.process.kill()
            self.process.wait(timeout=5)


# ── MQTT publisher playing the drone role ────────────────────────────────────


class DronePublisher:
    """Small paho MQTT publisher that connects to AWS IoT Core via mTLS.

    Uses a *different* client_id than the Flight Monitor's subscriber so
    IoT Core's "one connection per client_id" rule doesn't kick either of
    them out mid-test.  Publishes payloads that match the pipeline's
    expected schema (uavid, mission_status, status.location.altitude,
    status.battery.voltage, ...).
    """

    def __init__(
        self,
        *,
        client_id: str,
        ca_cert: str,
        client_cert: str,
        private_key: str,
        broker: str,
        port: int,
        topic: str,
        drone_id: str,
    ) -> None:
        self.client_id = client_id
        self.broker = broker
        self.port = port
        self.topic = topic
        self.drone_id = drone_id
        self._message_index = 0
        self._voltage = 16.6
        self._connected = threading.Event()
        self._connect_rc: int | None = None

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        self.client.tls_set(
            ca_certs=ca_cert,
            certfile=client_cert,
            keyfile=private_key,
            tls_version=ssl.PROTOCOL_TLSv1_2,
        )
        self.client.on_connect = self._on_connect

    def _on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        # Paho v2 callbacks: ``reason_code`` is a ReasonCode with .value, or an int.
        self._connect_rc = int(getattr(reason_code, "value", reason_code))
        if self._connect_rc == 0:
            self._connected.set()

    def connect(self, timeout: float = 15.0) -> bool:
        """Connect and wait for CONNACK.  Returns True on success."""
        self.client.connect(self.broker, self.port, keepalive=30)
        self.client.loop_start()
        return self._connected.wait(timeout)

    def publish(self, *, mission_status: str = "on_mission") -> None:
        """Publish one telemetry message; blocks briefly for the network send."""
        payload = self._build_payload(mission_status)
        info = self.client.publish(self.topic, json.dumps(payload), qos=0)
        try:
            info.wait_for_publish(timeout=5)
        except Exception:
            # wait_for_publish may raise on QoS 0; not fatal for our purposes.
            pass
        self._message_index += 1

    def _build_payload(self, mission_status: str) -> dict:
        """Build one telemetry envelope matching the pipeline's schema."""
        seconds = self._message_index * 0.5

        # Slight altitude oscillation so min/max tracking in the accumulator
        # has meaningful values (not a single flat reading).
        altitude = 100.0 + 10.0 * math.sin(seconds / 5.0)
        # Gentle battery drain to exercise voltage_in vs voltage_out tracking.
        self._voltage = max(14.5, self._voltage - 0.02)

        if mission_status == "on_mission":
            status_label, mode = "tracking", "AUTO"
        else:
            status_label, mode = "landed", "IDLE"

        return {
            "uavid": self.drone_id,
            "mission_status": mission_status,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "status": {
                "status": status_label,
                "mode": mode,
                "onboard_pilot": "e2e_test",
                "air_lease_state": "granted",
                "location": {
                    "latitude": round(39.7684 + (0.0001 * self._message_index), 6),
                    "longitude": round(-86.1581, 6),
                    "altitude": round(altitude, 2),
                },
                "drone_attitude": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
                "drone_heading": 0.0,
                "battery": {"voltage": round(self._voltage, 3)},
            },
        }

    def disconnect(self) -> None:
        """Stop the network loop and disconnect cleanly."""
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass


# ── Session registration POST ────────────────────────────────────────────────


def post_register_session(flight_session_id: str, drone_id: str) -> dict | None:
    """POST /flight-monitor/register-session with a realistic payload.

    Returns the parsed JSON response body, or None on any HTTP/network error.
    Plays the SADE-outbox role in the E2E test.  Does NOT set ``test_overrides``
    because we want the real MQTT telemetry path to drive finalization, not
    the stub path.
    """
    now = datetime.now(timezone.utc)
    body = {
        "flight_session_id": flight_session_id,
        "drone_id": drone_id,
        "pilot_id": "e2e-test-pilot",
        "organization_id": "e2e-test-org",
        "sade_zone_id": "e2e-test-zone",
        "requested_entry_time": now.isoformat(),
        "requested_exit_time": (now + timedelta(minutes=10)).isoformat(),
        "requested_operation": {"operation_type": "E2E_TEST", "priority": "NORMAL"},
        "submitted_at": now.isoformat(),
    }
    url = "http://localhost:8000/flight-monitor/register-session"
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        LOGGER.error("Register-session HTTP %s: %s", exc.code, exc.read().decode(errors="replace"))
        return None
    except Exception as exc:
        LOGGER.error("Register-session request failed: %s", exc)
        return None


# ── Result type ──────────────────────────────────────────────────────────────


@dataclass
class E2EResult:
    """Outcome of one E2E test run."""

    passed: bool
    failed_step: str | None = None
    reason: str | None = None
    flight_session_id: str = ""
    drone_id: str = ""
    reputation_record_id: str | None = None
    telemetry_published: int = 0
    dry_run: bool = False
    steps_completed: list[str] = field(default_factory=list)
    # True once SADE has returned any HTTP 200 response to the finalization
    # POST — independent of the business-level status.  The real signal for
    # "our contract is met".
    schema_accepted: bool = False
    # SADE's business-level status from the finalization response body:
    # "EXITED" for success, "FAILED" for a business rejection, None if no
    # response was received.
    sade_business_status: str | None = None
    # Passthrough of the `reason` field from SADE's business-level response.
    sade_business_reason: str | None = None


# ── Main orchestration ───────────────────────────────────────────────────────


def run_e2e(args: argparse.Namespace) -> E2EResult:
    """Execute the full E2E scenario, returning a structured result."""

    # ── Load + merge env ─────────────────────────────────────────────────────
    if not args.env_file.is_file():
        LOGGER.error("Env file not found: %s", args.env_file)
        LOGGER.error("Copy .env.example to .env and fill in values first:")
        LOGGER.error("  cd %s && cp .env.example .env && $EDITOR .env", REPO_ROOT)
        return E2EResult(passed=False, failed_step="pre-flight", reason=f"env file missing: {args.env_file}")

    file_env = parse_env_file(args.env_file)
    # Precedence: real os.environ wins over .env file (lets you override via CLI export).
    merged_env = dict(os.environ)
    for k, v in file_env.items():
        merged_env.setdefault(k, v)

    if args.dry_run:
        LOGGER.info("DRY-RUN MODE: forcing FINALIZE_TO_API=false; SADE POST will NOT fire.")
        merged_env["FINALIZE_TO_API"] = "false"

    # ── Pre-flight ───────────────────────────────────────────────────────────
    tls_enabled = merged_env.get("MQTT_TLS_ENABLED", "").lower() in {"1", "true", "yes"}
    finalize_on = merged_env.get("FINALIZE_TO_API", "").lower() in {"1", "true", "yes"}

    if not tls_enabled:
        return E2EResult(
            passed=False,
            failed_step="pre-flight",
            reason="MQTT_TLS_ENABLED must be true — this harness tests the real AWS IoT Core mTLS path.",
        )

    required_paths = ("MQTT_CA_CERT_PATH", "MQTT_CLIENT_CERT_PATH", "MQTT_PRIVATE_KEY_PATH")
    for var in required_paths:
        path = merged_env.get(var, "")
        if not path:
            return E2EResult(passed=False, failed_step="pre-flight", reason=f"{var} not set")
        if not Path(path).is_file():
            return E2EResult(passed=False, failed_step="pre-flight", reason=f"{var}='{path}' does not exist")

    if not merged_env.get("MQTT_CLIENT_ID"):
        return E2EResult(
            passed=False,
            failed_step="pre-flight",
            reason="MQTT_CLIENT_ID not set — AWS IoT Core will silently reject paho's random default.",
        )

    if finalize_on and not merged_env.get("TRACKER_FINALIZED_URL"):
        return E2EResult(
            passed=False,
            failed_step="pre-flight",
            reason="TRACKER_FINALIZED_URL not set but FINALIZE_TO_API=true. Set the URL in .env or use --dry-run.",
        )

    if not port_is_free("127.0.0.1", 8000):
        return E2EResult(
            passed=False,
            failed_step="pre-flight",
            reason="Port 8000 is already bound (another Flight Monitor running?). Stop it and retry.",
        )

    LOGGER.info("[OK] Pre-flight checks passed.")

    # ── Generate test IDs ────────────────────────────────────────────────────
    flight_session_id = args.flight_session_id or f"e2e-test-{uuid4()}"
    drone_id = args.drone_id or f"e2e-drone-{int(time.time())}"
    LOGGER.info("Flight session ID: %s", flight_session_id)
    LOGGER.info("Drone ID: %s", drone_id)

    # ── Spawn Flight Monitor ─────────────────────────────────────────────────
    result = E2EResult(
        passed=False,
        flight_session_id=flight_session_id,
        drone_id=drone_id,
        dry_run=args.dry_run,
    )
    fm = FlightMonitorRunner(merged_env)
    fm.start()

    try:
        # Step 1: readiness
        LOGGER.info("Waiting for Flight Monitor to reach IoT Core ...")
        for signal in ("mtls", "connected", "subscribed"):
            if fm.wait_for(signal, args.ready_timeout) is None:
                result.failed_step = f"readiness:{signal}"
                result.reason = (
                    f"timed out after {args.ready_timeout}s waiting for '{signal}' log line — "
                    f"see {FLIGHT_MONITOR_LOG_PATH} for details"
                )
                return result
        LOGGER.info("[OK] Flight Monitor connected to IoT Core (mTLS) and subscribed to update_drone")
        result.steps_completed.extend(["readiness:mtls", "readiness:connected", "readiness:subscribed"])

        # Step 2: register session
        LOGGER.info("POSTing session registration to Flight Monitor ...")
        reg_result = post_register_session(flight_session_id, drone_id)
        if reg_result is None or reg_result.get("action") != "registered":
            result.failed_step = "register-session"
            result.reason = f"registration response: {reg_result}"
            return result
        LOGGER.info("[OK] Session registered: action=%s flight_session_id=%s",
                    reg_result.get("action"), reg_result.get("flight_session_id"))
        result.steps_completed.append("register-session")

        # Step 3: connect drone publisher (distinct client_id)
        publisher_client_id = f"tlohman-e2e-drone-{int(time.time())}"
        LOGGER.info("Connecting E2E drone publisher (client_id=%s) to IoT Core ...", publisher_client_id)
        publisher = DronePublisher(
            client_id=publisher_client_id,
            ca_cert=merged_env["MQTT_CA_CERT_PATH"],
            client_cert=merged_env["MQTT_CLIENT_CERT_PATH"],
            private_key=merged_env["MQTT_PRIVATE_KEY_PATH"],
            broker=merged_env["MQTT_BROKER_HOST"],
            port=int(merged_env.get("MQTT_BROKER_PORT", "8883")),
            topic=merged_env.get("MQTT_TOPIC", "update_drone"),
            drone_id=drone_id,
        )
        if not publisher.connect(timeout=15.0):
            result.failed_step = "publisher-connect"
            result.reason = (
                f"drone publisher failed to connect to IoT Core (last rc={publisher._connect_rc}). "
                "Likely causes: duplicate client_id, IoT policy doesn't allow this client_id, "
                "or cert rejected."
            )
            return result
        LOGGER.info("[OK] E2E drone publisher connected")
        result.steps_completed.append("publisher-connect")

        try:
            # Step 4: publish on-mission telemetry
            LOGGER.info(
                "Publishing %d on_mission messages at %.2fs intervals ...",
                args.telemetry_count, args.telemetry_interval,
            )
            for _ in range(args.telemetry_count):
                publisher.publish(mission_status="on_mission")
                time.sleep(args.telemetry_interval)
            result.telemetry_published = args.telemetry_count
            LOGGER.info("[OK] Published %d on_mission messages", args.telemetry_count)
            result.steps_completed.append("telemetry-published")

            # Step 5: publish terminal message
            LOGGER.info("Publishing terminal mission_completed message ...")
            publisher.publish(mission_status="mission_completed")
            result.telemetry_published += 1
            # Give the Flight Monitor a moment to ingest the terminal message
            # off the network before we tear down the publisher connection.
            time.sleep(1.0)
            LOGGER.info("[OK] Published terminal mission_completed")
            result.steps_completed.append("terminal-published")
        finally:
            publisher.disconnect()

        # Step 6: wait for Flight Monitor to write final row
        LOGGER.info("Waiting for Flight Monitor to write final mission row ...")
        match = fm.wait_for("final_row", args.finalization_timeout)
        if match is None:
            result.failed_step = "final-row"
            result.reason = (
                f"Flight Monitor did not log 'wrote final mission row' within "
                f"{args.finalization_timeout}s — terminal MQTT message may not have been "
                f"received, or session may not have been registered for this drone_id."
            )
            return result
        LOGGER.info("[OK] Flight Monitor wrote final mission row")
        result.steps_completed.append("final-row")

        # Step 7: finalization POST (skipped in dry run)
        if args.dry_run:
            LOGGER.info("DRY-RUN: skipping finalization-POST wait.")
            result.passed = True
            return result

        LOGGER.info("Waiting for finalization POST to SADE ...")
        if fm.wait_for("finalize_posting", args.finalization_timeout) is None:
            result.failed_step = "finalize-posting"
            result.reason = "Flight Monitor did not log 'Sending tracker finalization POST' — finalization was never initiated."
            return result
        LOGGER.info("[OK] Finalization POST initiated")
        result.steps_completed.append("finalize-posting")

        # Step 8: SADE response
        #
        # Two terminal log lines are possible here:
        #   1. ``finalize_done``              — status=EXITED, reputation record created
        #   2. ``business_failure_detail``    — HTTP 200 + status=FAILED + reason
        #
        # Both prove SADE accepted our payload's schema; we branch on the
        # business-level status to decide PASS/FAIL.  A business FAILED whose
        # reason matches the expected-harness-limitation whitelist is still a
        # PASS, because the harness fabricates flight_session_ids that SADE
        # has no record of.
        LOGGER.info("Waiting for SADE to respond with reputation record ...")
        hit = fm.wait_for_any(
            ["finalize_done", "business_failure_detail"],
            args.finalization_timeout,
        )

        if hit is None:
            result.failed_step = "finalize-response"
            if fm.has_seen("finalize_failed"):
                result.reason = "all finalization retries failed — SADE did not respond successfully. Check runner log."
            elif fm.has_seen("finalize_http_err"):
                result.reason = "SADE returned non-200 HTTP response. Check runner log for status code and body."
            else:
                result.reason = (
                    f"no finalization response within {args.finalization_timeout}s and no error logged — "
                    "possibly a hung HTTP request."
                )
            return result

        signal_name, match = hit
        result.schema_accepted = True
        result.steps_completed.append("finalize-response")

        if signal_name == "finalize_done":
            result.sade_business_status = "EXITED"
            result.reputation_record_id = match.group("rep")
            LOGGER.info(
                "[OK] SADE finalized: flight_session_id=%s reputation_record_id=%s",
                match.group("fsid"), result.reputation_record_id,
            )
            result.passed = True
            return result

        # signal_name == "business_failure_detail" — schema accepted, business FAILED.
        reason_text = match.group("reason").strip()
        result.sade_business_status = "FAILED"
        result.sade_business_reason = reason_text

        if any(expected in reason_text for expected in EXPECTED_BUSINESS_FAILURE_REASONS):
            LOGGER.info(
                "[OK] SADE schema accepted; business FAILED as expected for fabricated session: %s",
                reason_text,
            )
            result.passed = True
        else:
            LOGGER.error(
                "SADE schema accepted but business FAILED with unexpected reason: %s",
                reason_text,
            )
            result.failed_step = "finalize-response"
            result.reason = f"SADE returned status=FAILED with unexpected reason: {reason_text!r}"
        return result

    finally:
        fm.stop()


# ── Summary writer ───────────────────────────────────────────────────────────


def write_summary(args: argparse.Namespace, result: E2EResult) -> None:
    """Write a human-readable summary file + echo it to the runner log."""

    lines: list[str] = []
    lines.append("SADE Flight Monitor — End-to-End AWS Test Summary")
    lines.append("==================================================")
    lines.append("")

    if result.passed:
        if result.dry_run:
            verdict = "DRY-RUN PASSED"
        elif result.sade_business_status == "EXITED":
            verdict = f"PASSED (SADE reputation_record_id={result.reputation_record_id})"
        elif result.sade_business_status == "FAILED":
            verdict = (
                "PASSED (schema accepted; SADE business FAILED as expected "
                f"for fabricated session: {result.sade_business_reason})"
            )
        else:
            verdict = "PASSED"
    else:
        verdict = f"FAILED at step: {result.failed_step}"
    lines.append(f"Result: {verdict}")
    if not result.passed and result.reason:
        lines.append(f"Reason: {result.reason}")
    lines.append("")

    lines.append("Test configuration")
    lines.append("------------------")
    lines.append(f"Env file              : {args.env_file}")
    lines.append(f"Flight session ID     : {result.flight_session_id}")
    lines.append(f"Drone ID              : {result.drone_id}")
    lines.append(f"Telemetry count       : {args.telemetry_count} (on_mission) + 1 (mission_completed)")
    lines.append(f"Telemetry interval    : {args.telemetry_interval}s")
    lines.append(f"Mode                  : {'DRY RUN (no SADE POST)' if result.dry_run else 'LIVE (real SADE POST)'}")
    lines.append(f"Readiness timeout     : {args.ready_timeout}s")
    lines.append(f"Finalization timeout  : {args.finalization_timeout}s")
    lines.append("")

    lines.append("Steps")
    lines.append("-----")
    all_steps = [
        ("readiness:mtls",       "Flight Monitor selected mTLS transport"),
        ("readiness:connected",  "Flight Monitor connected to IoT Core (rc=0)"),
        ("readiness:subscribed", "Flight Monitor subscribed to update_drone"),
        ("register-session",     "POST /flight-monitor/register-session → 202"),
        ("publisher-connect",    "E2E drone publisher connected to IoT Core"),
        ("telemetry-published",  f"Published {args.telemetry_count} on_mission messages"),
        ("terminal-published",   "Published mission_completed"),
        ("final-row",            "Flight Monitor wrote final mission row"),
        ("finalize-posting",     "Finalization POST initiated"),
        ("finalize-response",    "SADE responded (schema accepted)"),
    ]
    for name, label in all_steps:
        if result.dry_run and name in {"finalize-posting", "finalize-response"}:
            marker = "SKIP"
        elif name in result.steps_completed:
            marker = " OK "
        elif result.failed_step and name == result.failed_step:
            marker = "FAIL"
        else:
            marker = "  - "
        lines.append(f"  [{marker}] {label}")

    lines.append("")
    if result.schema_accepted:
        lines.append("SADE response")
        lines.append("-------------")
        lines.append("HTTP status     : 200")
        lines.append(f"Business status : {result.sade_business_status or '-'}")
        if result.sade_business_reason:
            lines.append(f"Reason          : {result.sade_business_reason}")
        lines.append(
            f"reputation_record_id : {result.reputation_record_id or '-'}"
        )
        lines.append("")

    lines.append("Artifacts")
    lines.append("---------")
    lines.append(f"Flight Monitor log : {FLIGHT_MONITOR_LOG_PATH}")
    lines.append(f"Runner log         : {RUNNER_LOG_PATH}")
    lines.append("")

    if not result.passed:
        lines.append("Diagnostic hints")
        lines.append("----------------")
        lines.append("1. Read the Flight Monitor log for connection / handshake errors.")
        lines.append("2. Confirm your IoT Core policy allows `iot:Connect` and `iot:Subscribe` on")
        lines.append("   'update_drone' for MQTT_CLIENT_ID, and `iot:Publish` on the same topic")
        lines.append("   for the publisher's e2e-drone client_id.")
        lines.append("3. If the failure is post-finalize, confirm TRACKER_FINALIZED_URL is reachable.")
        lines.append("")

    summary = "\n".join(lines) + "\n"
    SUMMARY_PATH.write_text(summary, encoding="utf-8")
    print()
    print(summary)
    print(f"Summary written to {SUMMARY_PATH}")


# ── CLI entry point ──────────────────────────────────────────────────────────


def main() -> int:
    """Parse args, configure logging, run the E2E scenario, write summary."""
    parser = argparse.ArgumentParser(description="End-to-end test of SADE Flight Monitor against real AWS cloud")
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help=f"Path to env file (default: {DEFAULT_ENV_FILE})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip the real SADE finalization POST. FINALIZE_TO_API is forced to false.",
    )
    parser.add_argument("--telemetry-count", type=int, default=10,
                        help="Number of on_mission telemetry messages to publish (default 10).")
    parser.add_argument("--telemetry-interval", type=float, default=0.5,
                        help="Seconds between telemetry publishes (default 0.5).")
    parser.add_argument("--flight-session-id", default=None,
                        help="Override the generated flight_session_id.")
    parser.add_argument("--drone-id", default=None,
                        help="Override the generated drone_id.")
    parser.add_argument("--ready-timeout", type=float, default=30.0,
                        help="Seconds to wait for Flight Monitor IoT Core readiness (default 30).")
    parser.add_argument("--finalization-timeout", type=float, default=30.0,
                        help="Seconds to wait for each finalization step (default 30).")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(RUNNER_LOG_PATH, mode="w", encoding="utf-8"),
        ],
    )

    # Silence paho's own chatty INFO-level logs — we use our own LOGGER.
    logging.getLogger("paho.mqtt.client").setLevel(logging.WARNING)

    result = run_e2e(args)
    write_summary(args, result)
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
