# SADE Telemetry Monitor

This service is the **flight monitor / telemetry tracker** component of the SADE system. It sits between an MQTT broker and the SADE AWS backend:

- It receives live telemetry from active drones over MQTT (AWS IoT Core via mTLS in production)
- It tracks mission state per drone in memory (altitude min/max, battery voltage, position, distance flown, message count)
- When SADE sends an exit-request (or registers with `test_overrides` for stub testing), the service sends a finalization report to the SADE AWS API (`POST /tracker-session-finalized`), which closes the approved session and records the reputation entry. Terminal MQTT messages from the drone are informational only — they trigger the local CSV diagnostic but do not close the session, since SADE owns finalization authority.

The finalization payload follows the schema in [SADE_AWS_API_INFORMATION/SADE_CONTRACT.md](SADE_AWS_API_INFORMATION/SADE_CONTRACT.md): top-level `telemetry_summary` (altitude min/max + `distance_flown_m`) plus an `events` array of `FLIGHT_SEGMENT` / `EXIT_REQUEST` / `INCIDENT` entries. Per-segment battery state carries `system_charge_pct` plus a `slots[]` array of `{slot_id, voltage_v}` to support multi-slot drones.

The service does **not** make entry decisions. That is SADE's job. This service only watches flights that SADE has already approved, records what actually happened, and reports back what is monitored.


---

## How data flows through the system

```
SADE AWS API
    │
    │  POST /flight-monitor/register-session      (normal registration)
    │  POST /flight-monitor/exit-request          (drone left zone early)
    ▼
FastAPI Webhook Server              ← receives session lifecycle events from SADE
(app/api/server.py)
    │
    │  registers flight_session_id (and/or starts exit grace period)
    ▼
ActiveSessionRegistry               ← in-memory store of approved sessions
(app/monitoring/active_session_registry.py)
    │
    │  shared with workers
    ▼
asyncio.Queue  ◄─── MQTT Broker ◄─── Drone telemetry (paho-mqtt)
    │                (status_message + update_drone topics)
    ▼
Telemetry Worker(s)                 ← one or more async worker tasks
(app/ingestion/workers.py)
    │
    ├── lookup session in registry (drop message if not registered)
    ├── update DroneStateTracker (altitude min/max, voltage, position, count)
    │
    └── on terminal status (mission_completed / complete / done / ...):
    │       └── write CSV row (if --out is set) — local diagnostic only
    │           (session stays active; SADE owns finalization)
    │
    └── on exit-request grace period elapsed (5 min telemetry silence)
        OR test_overrides stub timer (5 s):
            │
            └── POST /tracker-session-finalized  (always, when SADE-driven)
                    │  (with up to 2 retries on 5xx / network errors,
                    │   exponential backoff [1s, 2s])
                    ▼
                SADE AWS API  →  closes session, writes reputation record
```

> **Exit-policy update (in progress):** Per [docs/EXIT_POLICY_DESIGN.md](docs/EXIT_POLICY_DESIGN.md), terminal MQTT status no longer POSTs to SADE — finalization authority belongs exclusively to SADE (via the exit-request webhook) and the `test_overrides` stub. A periodic sweeper for deadline-breach flagging and stranded-session cleanup is planned next.

### Key state accumulated per drone

Each active drone session tracks (see `DroneState` in [app/monitoring/state_tracker.py](app/monitoring/state_tracker.py)):

- `first_seen` / `last_seen` — wall-clock timestamps of first and last message
- `min_altitude` / `max_altitude` — running min/max from `status.location.altitude`
- `voltage_in` / `voltage_out` — battery voltage from first and most recent message
- `distance_flown_m` — great-circle distance accumulated across GPS fixes (haversine)
- `start_position` / `position` — GPS coordinates at takeoff and current
- `message_count` — total messages processed
- `mission_status` / `mode` — latest reported status and mode strings
- `exit_requested_at` / `exit_reason` — stamped on the first SADE exit-request; feeds the `EXIT_REQUEST` event in the finalization payload

Finalization (SADE POST) fires exactly once per session. Today there are three trigger cases:

| Case | Trigger | POST to SADE |
|---|---|---|
| A1 | Terminal MQTT status (`mission_completed` / `complete` / `done` / ...) | **No** — CSV row only; session stays active |
| A2 | Exit-request received + 5 min telemetry silence | Yes |
| A3 | Same as A2 but no telemetry was ever received (synthetic payload) | Yes |
| A4 | `test_overrides` stub mode — fires 5 s after registration | Yes |

State is removed from memory after the POST. A1 used to also POST and close the session, but that was removed because finalization authority belongs to SADE — drones can't self-close. The remaining gap is the memory leak when SADE never sends an exit-request and the drone goes silent; the planned periodic sweeper closes that gap (see [docs/EXIT_POLICY_DESIGN.md](docs/EXIT_POLICY_DESIGN.md)).

---

## Session modes

### `aws` mode (production)

Drones are only tracked if they have a pre-registered approved session in the `ActiveSessionRegistry`. Sessions enter the registry via the `POST /flight-monitor/register-session` webhook — payload shape per [SADE_AWS_API_INFORMATION/SADE_CONTRACT.md](SADE_AWS_API_INFORMATION/SADE_CONTRACT.md), implementation details (status codes, retry, sweeper) per [FLIGHT_MONITOR_CONTRACT.md](SADE_AWS_API_INFORMATION/FLIGHT_MONITOR_CONTRACT.md). SADE posts to this service after approving a flight.

If a telemetry message arrives for a drone with no registered session, it is silently dropped.

When running under `run_service()` (the production / container startup path), the mode is forced to `aws` regardless of the CLI/env value.

### `local` mode (testing shortcut)

The pipeline auto-creates a synthetic session for any drone that publishes a message, without requiring any approval webhook. Useful for quick telemetry tests when you don't want to stand up the full approval flow.

Set via `--session-source-mode local` or `SESSION_SOURCE_MODE=local`.

---

## API endpoints

All endpoints are served by the FastAPI app in `app/api/server.py` on port `8000` (configurable via `API_PORT`).

### `POST /flight-monitor/register-session`  — primary registration endpoint

Payload shape per [SADE_AWS_API_INFORMATION/SADE_CONTRACT.md](SADE_AWS_API_INFORMATION/SADE_CONTRACT.md); implementation behaviour per [FLIGHT_MONITOR_CONTRACT.md](SADE_AWS_API_INFORMATION/FLIGHT_MONITOR_CONTRACT.md). Registration implicitly means the session has been approved by SADE. Once registered, MQTT telemetry published by the drone will be accepted and tracked.

- `202 Accepted` — session registered, telemetry tracking active
- `409 Conflict` — the drone already has an active session, registration rejected

**Test overrides / stub finalization.** If the registration payload includes a non-null `test_overrides` object, the real MQTT telemetry path is bypassed. A background task waits `STUB_FINALIZATION_DELAY_SECONDS` (5s), builds a finalization payload from the overrides (filling missing fields with safe defaults), POSTs it to `/tracker-session-finalized`, and marks the session complete. This lets SADE exercise the full register → finalize roundtrip without a real drone.

### `POST /flight-monitor/exit-request`  — early-exit handling

Notifies the Flight Monitor that a drone has left the zone early (or the session otherwise needs to close out). Documented in full in `SADE_AWS_API_INFORMATION/EXIT_REQUEST_GUIDE.md`.

- `202 Accepted` — session found, grace period started
- `404 Not Found` — no active session for `flight_session_id` (already finalized or never registered); safe to retry

**Grace period behaviour:**
- Session stays active — MQTT telemetry continues to be accepted and tracked during the grace period.
- A background task checks every `EXIT_GRACE_CHECK_INTERVAL_SECONDS` (30s) whether new telemetry has arrived.
- If the drone keeps transmitting, a warning is logged each cycle and the silence timer resets.
- After `EXIT_GRACE_PERIOD_SECONDS` (300s / 5 min) of continuous silence, the accumulated state is finalized and POSTed to SADE.
- If the drone sends a terminal MQTT status during the grace period, the local CSV row is written but the session stays alive — the grace task continues running until the silence threshold elapses, then it finalizes.
- If no telemetry was ever received, a finalization with an empty telemetry summary is still sent so SADE can close the session.

### `GET /health`

Returns `{"status": "ok", "active_sessions": <count>, "sessions_past_deadline": <count>, "sessions_stranded": <count>}`. The two flag counters come from the periodic sweeper.

### `GET /dashboard`  — live drone-status web page

A self-contained HTML page (vanilla CSS + JS, no build step, no external dependencies) that renders the current registry + telemetry state grouped by zone. Polls `/dashboard/data` every 7 seconds. Shows per-drone status (FLYING / LANDED / WAITING / EXIT_REQUESTED / ACTIVE) plus deadline-breach and stranded flags. Read-only — exposes no controls. **Not authenticated** — same gap as the webhooks; do not expose to the public internet without auth in front.

### `GET /dashboard/data`  — JSON snapshot for the dashboard

Programmatic JSON shape backing the dashboard page. Suitable for any other monitoring tool that wants live state. Same in-memory snapshot, no caching.

---

## Running locally

### Prerequisites

```bash
pip install -r requirements.txt
# Also requires a running Mosquitto MQTT broker on localhost:1883
# Install: sudo apt install mosquitto  or  brew install mosquitto
```

### Run via env file (recommended)

The quickest way to launch the service — particularly against AWS IoT Core — is to copy the committed `.env.example` template, fill in your deployment values, and run `scripts/run_flight_monitor.sh`. The wrapper validates the config before launching (checks cert files exist, `MQTT_CLIENT_ID` is set when mTLS is on, `TRACKER_FINALIZED_URL` is set when finalization is on, etc.) and prints a one-screen summary so misconfiguration surfaces immediately instead of as a silent MQTT hang.

```bash
cp .env.example .env
$EDITOR .env                          # fill in broker, client ID, cert paths, SADE URL, ...
scripts/run_flight_monitor.sh         # uses ./.env by default
scripts/run_flight_monitor.sh .env.aws-staging   # or a non-default env file
```

`.env` is gitignored (`.env.example` is not) so your real values stay local.

### Start the service manually

If you'd rather not use the env-file wrapper, `run.py` reads the same env vars directly:

```bash
# Full service (FastAPI + MQTT pipeline). Forces aws session mode.
python run.py --finalize-to-api
```

The pipeline alone (no webhook server) is not directly exposed on the CLI — `run.py` always runs the combined service path (`app.main.run_service`). If you want the pipeline standalone for local testing, either:

- use `local` session mode so telemetry auto-creates sessions (no webhook needed), or
- run the FastAPI app alone with uvicorn (`uvicorn app.api.server:app --host 0.0.0.0 --port 8000`).

```bash
# Local mode smoke test — no approval step, sessions created automatically
SESSION_SOURCE_MODE=local python run.py --out mission_rows.csv
```

### Running tests

```bash
# Unit tests (fast, no external services required)
python -m pytest tests/unit

# Integration tests (each script boots its own in-process services)
python tests/integration/test_mqtt_telemetry_pipeline.py
python tests/integration/test_stub_finalization_override.py
python tests/integration/test_exit_request_grace_period.py
```

The unit suite covers the approval handler, exit handler, MQTT workers, active session registry, mission row builder, pipeline metrics, state tracker, and tracker finalizer. The integration scripts each exercise one end-to-end slice: MQTT telemetry → finalization, `test_overrides` stub path, and exit-request grace period.

### Live demo for presentations — `scripts/run_demo.py`

Walks an audience through the full system in one self-contained Python process: pre-flight, service boot, two drones registering, arming, flying (with a multi-segment arm/disarm/arm sequence on one drone), exit-request → grace-period finalize → SADE catcher receiving the payload, clean shutdown. The dashboard at `http://localhost:8000/dashboard` updates live throughout.

Pause-on-Enter between phases by default so the presenter can talk; pass `--auto` to run unattended with fixed pauses.

```bash
# Make sure mosquitto is running on localhost:1883 first.
brew services start mosquitto

# Interactive demo (Enter to advance between phases)
./venv/bin/python scripts/run_demo.py

# Auto-advance for a quick smoke run (~90s)
./venv/bin/python scripts/run_demo.py --auto

# Show every per-message worker log line
./venv/bin/python scripts/run_demo.py --verbose
```

The script overrides the sweeper / grace / force-close timing constants to short values (8s grace, 5s sweeper, 30s stranded threshold, 60s force-close) so every behaviour is observable in a few minutes. Production values are 5-10× larger.

**For presenters**: full runbook with phase-by-phase narration cues, what to point at on the dashboard, and recovery steps in [DEMO.md](DEMO.md).

### End-to-end test against real AWS (IoT Core + SADE)

`scripts/run_e2e_aws_test.py` exercises the complete production code path against real AWS infrastructure — mTLS to IoT Core, real outbound POST to SADE's `/tracker-session-finalized`. It spawns the Flight Monitor as a subprocess, plays SADE's outbox role by POSTing `/flight-monitor/register-session` locally, then plays the drone role by publishing real MQTT telemetry to IoT Core using the same mTLS cert (with a distinct `client_id`). Success means the Flight Monitor's log shows `Tracker session finalized: ... reputation_record_id=<id>` — i.e. SADE persisted a real reputation record.

Because this test writes a real record to SADE's database, it is **not** part of the pytest suite. Run it manually on branch merges or before major deploys, not per-commit.

**Prerequisites:**
- A configured `.env` with real cert paths, `MQTT_CLIENT_ID`, and `TRACKER_FINALIZED_URL` set to the SADE ALB endpoint (or use `--dry-run` to skip the real POST)
- Cert files present on disk at the configured paths
- Port 8000 free on localhost
- An IoT Core policy on your cert that allows `iot:Connect` / `iot:Subscribe` on every topic in `MQTT_TOPIC` (default: `status_message,update_drone`) for the Flight Monitor role, and `iot:Publish` on the topic the drone publishes to (default: `status_message`) for the drone role

**Run it:**

```bash
# Full end-to-end including real SADE POST
python scripts/run_e2e_aws_test.py

# Dry run — exercises everything up to but not including the SADE POST
python scripts/run_e2e_aws_test.py --dry-run

# Tuning knobs
python scripts/run_e2e_aws_test.py --telemetry-count 20 --telemetry-interval 0.25
```

Artifacts land in `local_test_output/`: `e2e_aws_summary.txt` with the step-by-step verdict, `e2e_aws_flight_monitor.log` (full subprocess output), and `e2e_aws_runner.log` (harness's own activity). Exit code 0 on pass, 1 on fail.

### Key CLI flags

| Flag | Default | Description |
|---|---|---|
| `--broker` | `localhost` | MQTT broker host |
| `--port` | `1883` | MQTT broker port |
| `--topic` | `status_message,update_drone` | MQTT topic(s) to subscribe to. Single name or comma-separated list. |
| `--out` | `mission_rows.csv` | Path for CSV output (omit to disable) |
| `--session-source-mode` | `local` | `local` or `aws` |
| `--finalize-to-api` | off | POST finalization to SADE AWS on mission complete |
| `--workers` | `1` | Number of async worker tasks |
| `--queue-size` | `10000` | Max buffered messages |
| `--shutdown-timeout` | `5s` | Seconds to wait for queue drain on shutdown |
| `--idle-warning-seconds` | `300s` | Warn when no MQTT messages received for this long |
| `--metrics-log-interval` | `30s` | How often to log queue depth and latency |
| `--log-level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

All flags also accept environment variables (e.g. `MQTT_BROKER`, `FINALIZE_TO_API=true`).

---

## Docker / containerized deployment

### Build

```bash
docker build -t sade-telemetry-monitor:latest .
```

### Run locally (against a local Mosquitto broker)

```bash
docker run --rm -p 8000:8000 \
  -e MQTT_BROKER_HOST=host.docker.internal \
  -e FINALIZE_TO_API=false \
  -e LOG_LEVEL=DEBUG \
  sade-telemetry-monitor:latest
```

`FINALIZE_TO_API=false` is what lets this local-run command start without a `TRACKER_FINALIZED_URL` — the pipeline only enforces the env var when it's actually going to POST to SADE.

`/health` is available at `http://localhost:8000/health` once the container starts.

### Run with `docker compose` — broker + service together

For self-contained local-stack testing with no host-installed broker:

```bash
docker compose up --build
```

Brings up an Eclipse Mosquitto broker on port `1883` and the Flight Monitor on port `8000`, wired together over the default compose network. The service defaults to `SESSION_SOURCE_MODE=local` and `FINALIZE_TO_API=false`, so any drone that publishes telemetry to the local broker gets a synthetic session and the pipeline never tries to POST to SADE. Override either via inline `-e` or by editing `docker-compose.yml`. Mosquitto's `allow_anonymous true` config lives at [local_testing/mosquitto.conf](local_testing/mosquitto.conf) and is bind-mounted into the broker container.

### Run in production / AWS

> **Breaking change (2026-04-22):** `TRACKER_FINALIZED_URL` is now a required env var whenever `FINALIZE_TO_API=true` (the container default). The previous release's hardcoded AWS ALB URL has been removed, and the container will **fail to start** if this variable is unset while finalization is enabled. Set it explicitly for every environment you deploy to (see the example below).

**Before running the command below**, place your PKI files in `~/sade-certs/` on the host (or wherever you prefer — adjust the `-v` mount accordingly). The directory must contain `CAs.crt`, `client.crt`, and `client.key`, or override the paths via `MQTT_CA_CERT_PATH`, `MQTT_CLIENT_CERT_PATH`, and `MQTT_PRIVATE_KEY_PATH`. See `SADE_AWS_API_INFORMATION/aws_iot_pki.md` for how to generate the cert/key pair. AWS IoT Core will reject the connection without a valid client cert — username/password alone will not work.

> **Heads up on `MQTT_CLIENT_ID`:** this is required for AWS IoT Core. The IoT policy attached to your cert restricts which client IDs are allowed, so paho's random default will be silently rejected and the MQTT CONNECT phase will hang with no error logged. Set it explicitly (see the example below) and make sure only one instance at a time uses a given ID.

```bash
docker run -d --restart unless-stopped \
  -p 8000:8000 \
  -v ~/sade-certs:/certs:ro \
  -e MQTT_BROKER_HOST=a3dpdfmwa109lg-ats.iot.us-east-2.amazonaws.com \
  -e MQTT_BROKER_PORT=8883 \
  -e MQTT_TLS_ENABLED=true \
  -e MQTT_CA_CERT_PATH=/certs/CAs.crt \
  -e MQTT_CLIENT_CERT_PATH=/certs/client.crt \
  -e MQTT_PRIVATE_KEY_PATH=/certs/client.key \
  -e MQTT_CLIENT_ID=tlohman-flight-monitor \
  -e MQTT_TOPIC=status_message,update_drone \
  -e SESSION_SOURCE_MODE=aws \
  -e FINALIZE_TO_API=true \
  -e TRACKER_FINALIZED_URL=http://your-sade-host.example.com/tracker-session-finalized \
  -e LOG_LEVEL=INFO \
  sade-telemetry-monitor:latest
```

### Environment variables

| Variable | Default in container | Notes |
|---|---|---|
| `MQTT_BROKER_HOST` | `localhost` | Required — set to your broker hostname |
| `MQTT_BROKER_PORT` | `1883` | Use `8883` when TLS is enabled |
| `MQTT_TOPIC` | `status_message,update_drone` | MQTT telemetry topic(s). Single value or comma-separated list. Pipeline subscribes to all listed topics. |
| `MQTT_TLS_ENABLED` | `false` | Set `true` for cloud/managed brokers |
| `MQTT_USERNAME` | _(unset)_ | Pass at runtime — do not bake into image |
| `MQTT_PASSWORD` | _(unset)_ | Pass at runtime — do not bake into image |
| `MQTT_CA_CERT_PATH` | `/certs/CAs.crt` | Path inside the container to the CA cert used to verify the broker. Required for AWS IoT Core mTLS. |
| `MQTT_CLIENT_CERT_PATH` | `/certs/client.crt` | Path inside the container to your signed client certificate. Required for AWS IoT Core mTLS. |
| `MQTT_PRIVATE_KEY_PATH` | `/certs/client.key` | Path inside the container to your private key. **Never bake key material into the image** — mount it via `-v /host/sade-certs:/certs:ro` or ECS secrets. Required for AWS IoT Core mTLS. |
| `MQTT_CLIENT_ID` | _(unset — **required for AWS IoT Core**)_ | Stable MQTT client identifier. AWS IoT Core policies typically restrict which client IDs a given cert may use; paho's random default will be silently rejected and the MQTT CONNECT will hang. Pick a deployment-specific ID (e.g. `tlohman-flight-monitor`). Only one connection may use a given ID at a time. |
| `SESSION_SOURCE_MODE` | `aws` | `aws` requires approval webhook; `local` auto-creates sessions |
| `FINALIZE_TO_API` | `true` | POST finalization to SADE AWS on mission complete |
| `TRACKER_FINALIZED_URL` | _(unset — **required** when `FINALIZE_TO_API=true`)_ | Full URL of the SADE `/tracker-session-finalized` endpoint for the target environment. Pipeline startup fails fast if unset while finalization is enabled. |
| `MISSION_ROWS_OUT` | `""` | Empty = CSV disabled. Set to a path inside a mounted volume to enable |
| `WORKER_COUNT` | `1` | Number of async telemetry worker tasks |
| `QUEUE_SIZE` | `10000` | Max buffered MQTT messages |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `API_HOST` | `0.0.0.0` | FastAPI bind host |
| `API_PORT` | `8000` | FastAPI bind port |

### Exposed port

`8000` — FastAPI webhook server:
- `POST /flight-monitor/register-session`
- `POST /flight-monitor/exit-request`
- `GET /health`

### AWS deployment notes

- **Flight-monitor endpoints must be reachable by SADE** — place the container behind an ALB or NLB so SADE can POST `/flight-monitor/register-session` and `/flight-monitor/exit-request` to this service
- **`TRACKER_FINALIZED_URL` must be set** — this is a required env var when `FINALIZE_TO_API=true` (the container default). Pass the full URL of the target environment's `/tracker-session-finalized` endpoint via ECS task definition env, `docker run -e`, or equivalent. The pipeline refuses to start if it's unset while finalization is enabled.
- **MQTT credentials** — pass `MQTT_USERNAME` and `MQTT_PASSWORD` via ECS task definition secrets or AWS Secrets Manager; never bake them into the image
- **PKI for AWS IoT Core** — mount your CA cert, client cert, and private key as a read-only volume at `/certs/` (`-v ~/sade-certs:/certs:ro`). The pipeline reads them via `MQTT_CA_CERT_PATH`, `MQTT_CLIENT_CERT_PATH`, and `MQTT_PRIVATE_KEY_PATH`. Never `COPY` key material into the image. For ECS, store the private key in AWS Secrets Manager and mount it through the `secrets` section of the task definition.
- **AWS IoT Core** — set `MQTT_TLS_ENABLED=true`, `MQTT_BROKER_PORT=8883`, and configure an IoT Core policy attached to your client cert that allows `iot:Connect`, `iot:Subscribe`, and `iot:Receive` on the telemetry topic
- **CSV output** — disabled by default in the container; mount an EFS volume and set `MISSION_ROWS_OUT=/data/mission_rows.csv` to enable it
- **Startup order** — the container connects to MQTT immediately on start; if the broker isn't reachable the pipeline logs an error but the FastAPI server still comes up and accepts registration events

---

## Repository structure

```
sade/
├── run.py                          # Thin entry point — calls app.main.main()
├── requirements.txt
├── Dockerfile
│
├── app/
│   ├── main.py                     # Wires all components, parses args, runs the combined service
│   │
│   ├── api/
│   │   ├── server.py               # FastAPI app — register-session, exit-request, /health
│   │   ├── approval_handler.py     # Pydantic model + logic for register-session
│   │   └── exit_handler.py         # Pydantic model + logic for exit-request events
│   │
│   ├── ingestion/
│   │   ├── mqtt_client.py          # Paho MQTT client, bridges callbacks → asyncio.Queue
│   │   └── workers.py              # Async worker: parse → session lookup → state update → finalize
│   │
│   ├── monitoring/
│   │   ├── active_session_registry.py   # In-memory registry of SADE-approved sessions
│   │   ├── entry_session_listener.py    # Bridge: approval events → registry registration
│   │   ├── state_tracker.py             # Per-session DroneState accumulator
│   │   ├── pipeline_metrics.py          # Queue depth, latency, throughput counters
│   │   ├── mission_row_builder.py       # Builds a CSV-ready row from DroneState
│   │   └── mission_row_schema.py        # Column definitions and default row shape
│   │
│   ├── sending/
│   │   └── tracker_finalizer.py    # POST /tracker-session-finalized to SADE AWS API, with retry
│   │
│   ├── common/
│   │   └── mission_row_writer.py   # Thread-safe CSV writer
│   │
│   └── missions/                   # Placeholder for future mission-plan integration
│
├── local_testing/
│   ├── drone_sim.py                # Simulates a drone: publishes MQTT telemetry + mission lifecycle
│   ├── mqtt_publisher_client       # Low-level MQTT publish helper (loaded via SourceFileLoader by drone_sim)
│   ├── missions/
│   │   ├── sample_mission.json     # Example mission waypoint data
│   │   └── fly_waypoints_mission.json
│   └── local_testing_run_guide.txt # Notes on running local tests manually
│
├── scripts/
│   ├── run_flight_monitor.sh       # Env-file launcher with pre-flight config validation
│   ├── run_demo.py                 # Live narrated demo — boots the system, walks 2 drones through arm/fly/disarm/exit
│   ├── run_e2e_aws_test.py         # End-to-end test against real AWS IoT Core + SADE
│   ├── run_stress_test.py          # Stress harness (configurable drone count / queue depth)
│   ├── run_stress_test_sweep.py    # Parameter sweep across stress configurations
│   └── run_worker_comparison.py    # Benchmarks 1/2/4 worker configurations
│
├── docs/
│   └── EXIT_POLICY_DESIGN.md       # Data-flow map and planned new exit-policy design
│
├── tests/
│   ├── unit/
│   │   ├── api/                    # test_approval_handler, test_exit_handler
│   │   ├── ingestion/              # test_workers
│   │   ├── monitoring/             # registry, mission row builder, metrics, state tracker
│   │   └── sending/                # test_tracker_finalizer
│   └── integration/
│       ├── test_mqtt_telemetry_pipeline.py     # full MQTT → finalization roundtrip
│       ├── test_stub_finalization_override.py  # test_overrides stub path
│       └── test_exit_request_grace_period.py   # exit-request + grace period
│
└── SADE_AWS_API_INFORMATION/       # SADE API reference docs
    ├── START_HERE.md
    ├── OPERATOR_LIFECYCLE.md
    ├── API_REFERENCE.md
    ├── FLIGHT_MONITOR_CONTRACT.md
    ├── SADE_CONTRACT.md            # Current finalization-payload schema (2026-04-22)
    ├── REFERENCE_TABLES.md         # Event types, incident codes, payload-component types
    ├── EXIT_REQUEST_GUIDE.md
    └── IDEMPOTENCY_RECOMMENDATIONS.md
```

---

## Design decisions

**Asyncio queue between MQTT and workers**
Paho MQTT runs its own thread. Rather than doing work inside the callback (which blocks that thread), messages are immediately placed on an `asyncio.Queue` via `loop.call_soon_threadsafe()`. Worker coroutines then consume from the queue at their own pace. This keeps ingestion throughput decoupled from processing speed and makes backpressure straightforward to observe.

**Single source of truth for sessions via module-level registry**
`app/api/server.py` holds a module-level `registry: ActiveSessionRegistry` instance. The pipeline's `run_pipeline()` receives this same object via `args.session_registry`. Both the webhook server and the worker tasks read/write the same object — no IPC, no database, no message passing needed for the local deployment case.

**CSV and API finalization are independent paths**
`--out` (CSV) and `--finalize-to-api` (AWS POST) are separate flags and can run together. This made it easy to validate the pipeline's output locally before wiring the real API, and keeps local test runs free of external dependencies.

**Session modes, not two codebases**
Rather than maintaining separate local and production entry points, the same pipeline supports both via `--session-source-mode`. `local` mode auto-creates sessions; `aws` mode requires pre-registration. The worker code path is identical after session resolution.

**Terminal status detection by string set**
Mission completion is detected by matching `mission_status` against a set of known terminal strings (`mission_completed`, `complete`, `done`, etc.) rather than a single hardcoded value. This tolerates minor variations across drone firmware without requiring schema changes.

**Finalization fires exactly once**
`DroneState.row_written` is set to `True` before any output path runs. This prevents double-writes if multiple messages with a terminal status arrive (e.g., from a retry or duplicate publish), without needing a lock.

**Blocking HTTP calls run in a thread**
`post_tracker_session_finalized()` uses `asyncio.to_thread()` to run `urllib.request.urlopen` without blocking the event loop. No additional HTTP library dependency needed — consistent with how the test harness's health-check polling works.

**Bounded retry on finalization POST**
`post_tracker_session_finalized()` retries up to 2 times with `[1s, 2s]` exponential backoff. 5xx responses and network/transport errors are retried; 4xx responses are not (the payload is invalid — retrying won't change the outcome). SADE deduplicates on `flight_session_id`, so retrying is safe. Business-level `FAILED` responses are logged but not retried.

**All finalization paths share one payload shape**
All three SADE-POSTing trigger cases (exit-request grace period with telemetry, exit-request grace period without telemetry, and `test_overrides` stub) converge on the same payload shape defined in [SADE_AWS_API_INFORMATION/SADE_CONTRACT.md](SADE_AWS_API_INFORMATION/SADE_CONTRACT.md): top-level `telemetry_summary` plus an `events` array. `build_finalization_payload()` builds from an accumulated `DroneState`; `build_stub_finalization_payload()` builds from the `test_overrides` dict. The no-telemetry grace-period case runs a synthetic `DroneState` through `build_finalization_payload()` rather than assembling the payload inline, so there is exactly one code path that produces the contract shape.

**Exit grace period keeps the session live**
On `exit-request`, the session is NOT removed up front — it stays in the registry so MQTT telemetry continues to be accepted and accumulated during the grace period. A background task polls every 30s and only finalizes after 5 minutes of continuous silence. This ensures we capture the drone's final telemetry as it leaves the zone rather than cutting off mid-flight.

---

## What still needs to be done

**Authentication on the webhook endpoints**
`/flight-monitor/register-session` and `/flight-monitor/exit-request` have no auth. Any caller that can reach the port can register or close out a session. For production, this needs at minimum a shared secret header check, ideally mTLS or IAM-signed requests depending on the deployment environment.

**Battery `system_charge_pct` and multi-slot voltages**
Per-segment `battery_state_in` / `battery_state_out` currently emit `system_charge_pct: 0.0` and wrap the single telemetry voltage as `slots: [{slot_id: "A", voltage_v: <v>}]`. Two firmware-side gaps block real data here: (1) telemetry does not yet carry a battery percentage field — either firmware adds one or the service needs a voltage-to-percentage curve per battery type; (2) multi-slot drones do not yet emit per-slot voltages, so `slots` is always a single-entry array today. The builder in `app/sending/tracker_finalizer.py` has a single `_build_battery_state()` helper — that is the one-line change point when either gap closes. It is also worth confirming with SADE whether `system_charge_pct: null` is acceptable; if so, the `0.0` placeholder flips to `null` until real data is available.

**CSV output is not production output**
The CSV writer was built for local observation and debugging. The finalization API call (`--finalize-to-api`) is the production path. The CSV should be treated as a diagnostic tool, not a record of truth.

**Worker count tuning**
The pipeline defaults to 1 async worker. The `scripts/run_worker_comparison.py` benchmark showed diminishing returns beyond 1–2 workers at current message rates since the bottleneck is I/O bound, but this should be validated under real production load with concurrent drones.

**Mission-plan integration**
`app/missions/` is a placeholder. The drone simulator uses static waypoint JSON files but the production service has no awareness of planned vs. actual flight paths. Route deviation and waypoint compliance are not computed (basic total distance flown is — via haversine accumulation in `DroneStateTracker`).

**Sweeper-based session backstop** *(implemented)*
Sessions that never receive an exit-request used to leak in memory until process restart. A periodic background sweeper now scans the registry every 60 s and (1) flags sessions past `requested_exit_time` without an exit-request as `exit_deadline_breached_at`, (2) flags sessions whose telemetry has been silent >10 min without an exit-request as `stranded_flagged_at`, and (3) force-closes any session that has been carrying a flag for >24 h via the canonical finalize sequence. Both flags are one-shot edge detectors and surfaced on `/health` as `sessions_past_deadline` / `sessions_stranded`. Terminal MQTT statuses still do not close sessions — SADE owns finalization. Trigger/timing details in [SADE_AWS_API_INFORMATION/FLIGHT_MONITOR_CONTRACT.md](SADE_AWS_API_INFORMATION/FLIGHT_MONITOR_CONTRACT.md); original design narrative in [docs/EXIT_POLICY_DESIGN.md](docs/EXIT_POLICY_DESIGN.md).

**Incident detection**
`INCIDENT` events are defined in [SADE_AWS_API_INFORMATION/REFERENCE_TABLES.md](SADE_AWS_API_INFORMATION/REFERENCE_TABLES.md) with a standard `hhhh-sss` code format, but the Flight Monitor does not yet emit any. Detection of the relevant incident categories (airspace violation, loss-of-control, battery failure, etc.) requires a signal-to-code mapping that is not yet specified.

**Multi-segment `FLIGHT_SEGMENT` detection** *(implemented)*
Sessions now emit one `FLIGHT_SEGMENT` event per arm/disarm window observed during the session, driven by the firmware-emitted `status.armed` boolean. A drone that takes off, lands to recharge, then takes off again produces two `FLIGHT_SEGMENT` events with their own `time_in_utc`/`time_out_utc` and per-segment `battery_state_in`/`battery_state_out`. A session whose drone never arms produces zero `FLIGHT_SEGMENT` events (the truthful "powered on but didn't fly" answer). Older firmware that doesn't emit `status.armed` falls back to the legacy "one synthetic segment per session" shape so it stays contract-valid. Any segment still open at finalize time is auto-closed at `state.last_seen` inside `build_finalization_payload`.
