# SADE Telemetry Monitor

This service is the **flight monitor / telemetry tracker** component of the SADE system. It sits between an MQTT broker and the SADE AWS backend:

- It receives live telemetry from active drones over MQTT
- It tracks mission state per drone in memory (altitude, battery, position, message count)
- When a mission completes, it sends a finalization report to the SADE AWS API (`POST /tracker-session-finalized`), which closes the approved session and records the reputation entry

The service does **not** make entry decisions. That is SADE's job. This service only watches flights that SADE has already approved, records what actually happened, and reports back when they land.

---

## How data flows through the system

```
SADE AWS API
    │
    │  POST /entry-approval
    ▼
FastAPI Webhook Server              ← receives approval events from SADE
(app/api/server.py)
    │
    │  registers flight_session_id
    ▼
ActiveSessionRegistry               ← in-memory store of approved sessions
(app/monitoring/active_session_registry.py)
    │
    │  shared with workers
    ▼
asyncio.Queue  ◄─── MQTT Broker ◄─── Drone telemetry (paho-mqtt)
    │                (update_drone topic)
    ▼
Telemetry Worker(s)                 ← one or more async worker tasks
(app/ingestion/workers.py)
    │
    ├── lookup session in registry (drop message if not registered)
    ├── update DroneStateTracker (altitude min/max, voltage, position, count)
    │
    └── on terminal status (mission_completed / complete / done / ...):
            │
            ├── write CSV row (if --out is set)
            │
            └── POST /tracker-session-finalized  (if --finalize-to-api)
                    │
                    ▼
                SADE AWS API  →  closes session, writes reputation record
```

### Key state accumulated per drone

Each active drone session tracks:
- `first_seen` / `last_seen` — wall-clock timestamps of first and last message
- `min_altitude` / `max_altitude` — running min/max from `status.location.altitude`
- `voltage_in` / `voltage_out` — battery voltage from first and most recent message
- `start_position` / `position` — GPS coordinates at takeoff and current
- `message_count` — total messages processed
- `mission_status` — latest reported status string

Finalization fires exactly once per session when a terminal status arrives. The state is then removed from memory.

---

## Session modes

### `aws` mode (production)

Drones are only tracked if they have a pre-registered approved session in the `ActiveSessionRegistry`. Sessions enter the registry via the `POST /entry-approval` webhook, which SADE posts to this service after approving a flight.

If a telemetry message arrives for a drone with no registered session, it is silently dropped.

### `local` mode (testing shortcut)

The pipeline auto-creates a synthetic session for any drone that publishes a message, without requiring any approval webhook. Useful for quick telemetry tests when you don't want to stand up the full approval flow.

Set via `--session-source-mode local` or `SESSION_SOURCE_MODE=local`.

---

## Running locally

### Prerequisites

```bash
pip install -r requirements.txt
# Also requires a running Mosquitto MQTT broker on localhost:1883
# Install: sudo apt install mosquitto  or  brew install mosquitto
```

### Start the pipeline manually

```bash
# aws mode: drones must enter through the approval API first
python run.py --session-source-mode aws --finalize-to-api

# local mode: no approval step, sessions created automatically
python run.py --session-source-mode local --out mission_rows.csv
```

### Run the automated local test (recommended)

The `scripts/run_local_test.py` script starts everything — Mosquitto, the FastAPI approval server, the ingestion pipeline, and simulated drones — wired together and self-contained.

```bash
# 3 drones, 60 seconds each, entering through the approval API, reporting to AWS
python scripts/run_local_test.py \
  --drone-count 3 \
  --publisher-runtime-seconds 60 \
  --finalize-to-api

# Quick smoke test — local session mode, no AWS call
python scripts/run_local_test.py \
  --drone-count 2 \
  --publisher-runtime-seconds 30 \
  --skip-approval-api \
  --session-source-mode local
```

Output CSVs and logs land in `local_test_output/`.

### Key CLI flags

| Flag | Default | Description |
|---|---|---|
| `--broker` | `localhost` | MQTT broker host |
| `--port` | `1883` | MQTT broker port |
| `--topic` | `update_drone` | MQTT topic to subscribe to |
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

`/health` is available at `http://localhost:8000/health` once the container starts.

### Run in production / AWS

```bash
docker run -d --restart unless-stopped \
  -p 8000:8000 \
  -e MQTT_BROKER_HOST=your-broker.example.com \
  -e MQTT_BROKER_PORT=8883 \
  -e MQTT_TLS_ENABLED=true \
  -e MQTT_USERNAME=your-username \
  -e MQTT_PASSWORD=your-password \
  -e MQTT_TOPIC=update_drone \
  -e SESSION_SOURCE_MODE=aws \
  -e FINALIZE_TO_API=true \
  -e LOG_LEVEL=INFO \
  sade-telemetry-monitor:latest
```

### Environment variables

| Variable | Default in container | Notes |
|---|---|---|
| `MQTT_BROKER_HOST` | `localhost` | Required — set to your broker hostname |
| `MQTT_BROKER_PORT` | `1883` | Use `8883` when TLS is enabled |
| `MQTT_TOPIC` | `update_drone` | MQTT telemetry topic |
| `MQTT_TLS_ENABLED` | `false` | Set `true` for cloud/managed brokers |
| `MQTT_USERNAME` | _(unset)_ | Pass at runtime — do not bake into image |
| `MQTT_PASSWORD` | _(unset)_ | Pass at runtime — do not bake into image |
| `SESSION_SOURCE_MODE` | `aws` | `aws` requires approval webhook; `local` auto-creates sessions |
| `FINALIZE_TO_API` | `true` | POST finalization to SADE AWS on mission complete |
| `MISSION_ROWS_OUT` | `""` | Empty = CSV disabled. Set to a path inside a mounted volume to enable |
| `WORKER_COUNT` | `1` | Number of async telemetry worker tasks |
| `QUEUE_SIZE` | `10000` | Max buffered MQTT messages |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `API_HOST` | `0.0.0.0` | FastAPI bind host |
| `API_PORT` | `8000` | FastAPI bind port |

### Exposed port

`8000` — FastAPI webhook server (`POST /entry-approval`, `GET /health`)

### AWS deployment notes

- **`/entry-approval` must be reachable by SADE** — place the container behind an ALB or NLB so SADE can POST approval events to this endpoint
- **MQTT credentials** — pass `MQTT_USERNAME` and `MQTT_PASSWORD` via ECS task definition secrets or AWS Secrets Manager; never bake them into the image
- **AWS IoT Core** — set `MQTT_TLS_ENABLED=true`, `MQTT_BROKER_PORT=8883`, and configure an IoT Core policy to allow subscribe on the telemetry topic
- **CSV output** — disabled by default in the container; mount an EFS volume and set `MISSION_ROWS_OUT=/data/mission_rows.csv` to enable it
- **Startup order** — the container connects to MQTT immediately on start; if the broker isn't reachable the pipeline logs an error but the FastAPI server still comes up and accepts approval events

---

## Repository structure

```
sade/
├── run.py                          # Thin entry point — calls app.main.main()
├── requirements.txt
│
├── app/
│   ├── main.py                     # Wires all components, parses args, runs asyncio pipeline
│   │
│   ├── api/
│   │   ├── server.py               # FastAPI app — POST /entry-approval, GET /health
│   │   └── approval_handler.py     # Pydantic model + business logic for approval events
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
│   │   └── tracker_finalizer.py    # POST /tracker-session-finalized to SADE AWS API
│   │
│   ├── common/
│   │   └── mission_row_writer.py   # Thread-safe CSV writer
│   │
│   └── missions/                   # Placeholder for future mission-plan integration
│
├── local_testing/
│   ├── drone_sim.py                # Simulates a drone: publishes MQTT telemetry + mission lifecycle
│   ├── mqtt_publisher_client       # Low-level MQTT publish helper used by drone_sim
│   ├── drone_mqtt_client.py        # Standalone drone MQTT client
│   └── local_testing_run_guide.txt # Notes on running local tests manually
│
├── scripts/
│   ├── run_local_test.py           # Full local integration test harness
│   └── run_worker_comparison.py    # Benchmarks 1/2/4 worker configurations
│
├── missions/
│   ├── sample_mission.json         # Example mission waypoint data
│   └── fly_waypoints_mission.json
│
├── tests/                          # Test directory (currently empty)
│
└── SADE_AWS_API_INFORMATION/       # SADE API reference docs
    ├── START_HERE.md
    ├── OPERATOR_LIFECYCLE.md
    ├── API_REFERENCE.md
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

---

## What still needs to be done

**Real SADE approval webhook integration**
Currently the `POST /entry-approval` endpoint is called by the local test harness with synthetic payloads. In production, SADE needs to be configured to POST to this service's `/entry-approval` endpoint when a drone session is approved. The payload shape and auth mechanism (if any) need to be confirmed against the live SADE webhook contract.

**Authentication on the approval endpoint**
The `/entry-approval` endpoint has no auth. Any caller can register a session. For production, this needs at minimum a shared secret header check, ideally mTLS or IAM-signed requests depending on the deployment environment.

**Battery percentage fields**
`battery_start_pct` and `battery_end_pct` are sent as `0.0` in every finalization report. The current telemetry payload only carries voltage, not percentage. Either the drone firmware needs to add a percentage field, or the service needs a voltage-to-percentage conversion curve per battery type.

**CSV output is not production output**
The CSV writer was built for local observation and debugging. The finalization API call (`--finalize-to-api`) is the production path. The CSV should be treated as a diagnostic tool, not a record of truth.

**No unit or integration tests**
The `tests/` directory exists but is empty. The local test harness (`scripts/run_local_test.py`) covers the happy path end-to-end, but there are no unit tests for the worker parsing logic, state accumulator, finalization payload builder, or approval handler.

**Worker count tuning**
The pipeline defaults to 1 async worker. The `scripts/run_worker_comparison.py` benchmark showed diminishing returns beyond 1–2 workers at current message rates since the bottleneck is I/O bound, but this should be validated under real production load with concurrent drones.

**No `docker-compose.yml` for local full-stack testing**
A `Dockerfile` exists and the image builds and runs. A `docker-compose.yml` that spins up Mosquitto + this service together would make local Docker-based testing self-contained without needing a host-installed broker.

**Mission-plan integration**
`app/missions/` is a placeholder. The drone simulator uses static waypoint JSON files but the production service has no awareness of planned vs. actual flight paths. Distance flown, route deviation, and waypoint compliance are not computed.

**Idle drone cleanup**
If a drone registers a session and then goes silent without ever sending a terminal status (e.g., crash, lost comms), its state lives in memory indefinitely. There is no TTL or watchdog that cleans up stale in-flight sessions.
