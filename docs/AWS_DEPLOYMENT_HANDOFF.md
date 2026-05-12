# AWS Deployment Handoff — Flight Monitor

Handoff reference for the AWS-side work needed to put the Flight Monitor
into production. Code is complete and shipped on `main`; everything in
this doc is infrastructure work owned by whoever runs the AWS
environment.

The current `main` commit is the image source. Tag the ECR
push with the commit SHA so the deployed image traces back to a known
state.

---

## At a glance

```
SADE outbox  ──HTTPS+mTLS──►  [ ALB / NLB ]  ──►  ECS task (container)  ──HTTPS+mTLS──►  api.sadezone.org
                                                       │
                                                       │ MQTT+mTLS
                                                       ▼
                                              AWS IoT Core (8883)
```

One ECS task, one ALB (or NLB) in front, two outbound endpoints (SADE's
finalize endpoint and AWS IoT Core). Single replica — the Flight
Monitor keeps active-session state in memory.

---

## 1. PKI / certs to issue and supply

The Flight Monitor needs **one** "systems" identity cert that serves
three purposes: server cert presented to inbound callers, client cert
presented to SADE on outbound finalize POSTs, and the corresponding
private key. Plus two CA bundles for the verify sides.

| Item | Purpose | Notes |
|---|---|---|
| `flight-monitor.crt` | Systems identity — presented inbound AND outbound | Must be signed by a CA that `api.sadezone.org` will trust as a client. Coordinate with whoever runs SADE's PKI. Suggested CN: `flight-monitor-systems` (or whatever your IoT/PKI policy allows). |
| `flight-monitor.key` | Private key matching above | **Never commit, never bake into the image.** Store in AWS Secrets Manager. |
| `api-clients-ca.crt` | CA bundle used to verify inbound clients | Whatever CA signs the certs SADE's outbox presents when calling the Flight Monitor's endpoints. May or may not be the same CA as the systems cert is signed by. |
| `sade-server-ca.crt` *(optional)* | CA bundle used to verify `api.sadezone.org`'s server cert | Leave unset if `api.sadezone.org` uses a publicly-trusted cert (ACM, Let's Encrypt) — the container's system trust store handles it. Set only when SADE uses a private/internal CA. |

MQTT mTLS for AWS IoT Core uses a separate cert (already working in
dev). The Flight Monitor's `MQTT_CLIENT_ID` must be allowed by an IoT
policy attached to that cert; paho's random default will be silently
rejected.

---

## 2. AWS resources to provision

1. **ECR repo** — e.g. `flight-monitor`. Build and push the image from
   the current `main` HEAD, tagged with the commit SHA.

2. **AWS Secrets Manager** — two secrets:
   - `flight-monitor/systems-key` — contents of `flight-monitor.key`
   - `flight-monitor/mqtt-key` — contents of the AWS IoT Core client
     private key

   The cert chains themselves (`.crt` files) are non-secret and can be
   baked into a sidecar config or mounted via a volume — only the
   private keys need Secrets Manager.

3. **ECS task definition** — see §3 for env vars and §4 for the
   mTLS-termination decision. Configure:
   - Container image from ECR (tag = commit SHA)
   - Env vars and `secrets` block per §3
   - Log driver: `awslogs` → CloudWatch group `/ecs/flight-monitor`
   - CPU/memory: TBD — run [scripts/run_stress_test.py](../scripts/run_stress_test.py)
     at expected concurrent-drone count before locking in numbers

4. **ECS service** — `desiredCount: 1`. The Flight Monitor keeps
   in-memory state (`ActiveSessionRegistry`, `DroneStateTracker`);
   running multiple replicas would race on registration/exit-request
   webhooks. Restart policy: replace task on non-zero exit. The
   pipeline-crash done-callback already exits 1 cleanly on any
   internal failure, so this works automatically.

5. **ALB or NLB** in front of port 8000 — see §4 for the mTLS-
   termination choice.

6. **CloudWatch log group** — `/ecs/flight-monitor`. Container already
   logs structured to stdout; the awslogs driver picks it up directly.

7. **DNS** — friendly hostname (e.g. `flight-monitor.sadezone.org`)
   pointing at the ALB so SADE's outbox calls it by a stable name, not
   the auto-generated ELB DNS. Same reasoning as why we moved off the
   raw ELB hostname for `TRACKER_FINALIZED_URL`.

8. **Security groups**:
   - **Container ingress**: port 8000 from the ALB only.
   - **Container egress**: 8883 to the AWS IoT Core ATS endpoint, 443
     to `api.sadezone.org`. Nothing else.
   - **ALB ingress**: 443 from SADE's outbox source range (or VPC-
     internal only if SADE lives in the same VPC).

---

## 3. ECS task definition configuration

### Env vars (plain values — safe in the task definition)

```
# MQTT (AWS IoT Core)
MQTT_BROKER_HOST       = <your-iot-core-ats-endpoint>.iot.us-east-2.amazonaws.com
MQTT_BROKER_PORT       = 8883
MQTT_TLS_ENABLED       = true
MQTT_TOPIC             = status_message,update_drone
MQTT_CLIENT_ID         = flight-monitor-prod                       # must be allowed by the IoT policy
MQTT_CA_CERT_PATH      = /certs/AmazonRootCA.crt
MQTT_CLIENT_CERT_PATH  = /certs/iot-client.crt
MQTT_PRIVATE_KEY_PATH  = /certs/iot-client.key                     # → mounted from Secrets Manager

# Session source and outbound finalize
SESSION_SOURCE_MODE    = aws
FINALIZE_TO_API        = true
TRACKER_FINALIZED_URL  = https://api.sadezone.org/tracker-session-finalized

# FastAPI server
API_HOST               = 0.0.0.0
API_PORT               = 8000

# API mTLS — leave the three API_* cert vars UNSET if terminating mTLS at the ALB (§4 Option A)
API_CA_CERT_PATH       = /api-certs/api-clients-ca.crt
API_SERVER_CERT_PATH   = /api-certs/flight-monitor.crt
API_SERVER_KEY_PATH    = /api-certs/flight-monitor.key             # → mounted from Secrets Manager

# Optional — leave UNSET unless api.sadezone.org uses a private CA
# TRACKER_CA_CERT_PATH = /api-certs/sade-server-ca.crt

LOG_LEVEL              = INFO
```

### Secrets (from Secrets Manager)

The two private keys (`MQTT_PRIVATE_KEY_PATH` and `API_SERVER_KEY_PATH`)
must be mounted via the task definition's `secrets` block, not as plain
env vars. The env vars above only carry the *paths* — the container
reads the actual key material from disk via `ssl.load_cert_chain`.

Example task-definition `secrets` snippet (sketch):

```jsonc
"secrets": [
  { "name": "...", "valueFrom": "arn:aws:secretsmanager:...:secret:flight-monitor/systems-key" },
  { "name": "...", "valueFrom": "arn:aws:secretsmanager:...:secret:flight-monitor/mqtt-key"  }
]
```

Wire the Secrets Manager values to files at the paths the env vars
point at (`/api-certs/flight-monitor.key`, `/certs/iot-client.key`).
Common patterns: an init container that writes the secret to an
`emptyDir` volume, or the ECS-native `secrets` → file mount.

### Full env-var contract

[README.md](../README.md#environment-variables) has the canonical
env-var table with defaults, descriptions, and which are required for
which mode. Wire-level shapes for inbound webhooks, MQTT telemetry,
and the outbound finalize POST are in
[docs/MESSAGE_SHAPES.md](MESSAGE_SHAPES.md).

---

## 4. mTLS termination decision (open)

Two valid options for the inbound ALB. Both work with the code we
shipped — the pipeline auto-detects which mode based on whether the
three `API_*` cert env vars are set.

### Option A — terminate mTLS at the ALB

ALB does the mTLS handshake against an ACM trust store, forwards plain
HTTP to the container on port 8000.

- Simpler ops; standard AWS pattern.
- Requires uploading the `api-clients-ca.crt` to ACM as a "trust store"
  and attaching it to the ALB listener.
- In this mode, leave **all three** `API_CA_CERT_PATH`,
  `API_SERVER_CERT_PATH`, `API_SERVER_KEY_PATH` env vars **unset** so
  uvicorn serves plain HTTP (the ALB has already authenticated the
  caller).
- The cert resolver in [app/main.py](../app/main.py) accepts this
  configuration: all three unset → plain HTTP, no error.

### Option B — TLS passthrough via NLB

Use a Network Load Balancer (ALB cannot do TLS passthrough). NLB
forwards encrypted TCP straight to the container, which does its own
mTLS via the configured certs.

- Keeps the container as the security boundary; survives an ALB
  misconfig that might otherwise expose plain HTTP.
- Set all three `API_*` cert env vars in this mode.
- Health check probe must speak TLS to the container.

### Recommendation

Either is fine. I like option A better.

---

## 5. Outbound mTLS to SADE

SADE enforces client-cert auth on `api.sadezone.org`. The Flight
Monitor's outbound POST to `/tracker-session-finalized` automatically
presents `API_SERVER_CERT_PATH` + `API_SERVER_KEY_PATH` as the client
cert when both are set — same systems identity used inbound (Option B)
or as the dedicated outbound identity (Option A, with the API_* vars
set even though uvicorn isn't using them server-side).

In **Option A**, the `API_SERVER_CERT_PATH` + `API_SERVER_KEY_PATH`
vars must still be set in the container even though uvicorn doesn't use
them — they're required by the outbound finalize path. Don't set
`API_CA_CERT_PATH` in this mode (it would re-enable inbound mTLS at the
container, which conflicts with terminating at the ALB).

Verification:
- Startup log should read:
  `Outbound finalization mTLS enabled: client_cert=/api-certs/flight-monitor.crt ca=(system trust store) url=https://api.sadezone.org/...`
- If it reads `HTTPS (server-auth only — no client cert)`, the systems
  cert isn't reaching the container — SADE will reject every POST.

---

## 6. Verification after deploy

1. **Container starts cleanly.** CloudWatch logs show:
   - `Session sweeper started`
   - `Connecting to MQTT broker` then `Connected to MQTT broker (rc=0)`
   - `Subscribed to topics: status_message, update_drone`
   - `Pipeline started: ...`
   - Either `API mTLS enabled` (Option B) or `API mTLS not configured — serving plain HTTP` (Option A)
   - `Outbound finalization mTLS enabled: client_cert=...` — this line is the critical one for SADE auth

2. **`/health` returns 200** through the ALB. Body should look like:
   `{"status":"ok","active_sessions":0,"sessions_past_deadline":0,"sessions_stranded":0}`.

3. **`/dashboard`** renders in a browser (only meaningful in Option B
   where the dashboard is served over HTTPS; in Option A it's plain
   HTTP behind the ALB).

4. **End-to-end smoke test** — easiest path:
   - SADE sends a registration with a `test_overrides` payload.
   - 5 s later the Flight Monitor POSTs to
     `https://api.sadezone.org/tracker-session-finalized`.
   - SADE returns `200 OK` with `{"status": "EXITED", ...}`.
   - Flight Monitor logs: `POST https://api.sadezone.org/... → HTTP 200 ...`
   - `/health` returns to `active_sessions=0`.

   If the POST fails with a TLS error, the systems cert isn't being
   presented or isn't trusted by SADE — verify the cert is in
   `/api-certs/flight-monitor.crt` inside the container and was issued
   by a CA `api.sadezone.org` trusts.

5. **Resource sizing.** Run
   [scripts/run_stress_test.py](../scripts/run_stress_test.py) against
   the deployed container at expected concurrent-drone count before
   going live. Stress test wires up its own ephemeral broker and
   publishes simulated telemetry — output includes peak memory, queue
   depth, and message-processing latency. Use the peak memory + 50%
   headroom to set ECS memory limits.

---

## 7. Operational notes

- **Single replica.** As above — the Flight Monitor keeps in-memory
  state. ECS service `desiredCount` must be `1`. Multiple replicas
  would race on registration/exit-request webhooks.

- **Restart on failure.** ECS should restart the task on non-zero exit.
  Our pipeline-crash done-callback exits the container with code 1
  cleanly on any unrecoverable error (MQTT broker unreachable for too
  long, pipeline coroutine crashed, etc.). The done-callback logs a
  full traceback to CloudWatch before exiting.

- **Force-close backstop.** The internal session sweeper force-closes
  any session that has been carrying a deadline-breach or stranded flag
  for >24 h, even without an exit-request from SADE. This is a memory-
  safety backstop; a session is never leaked permanently. See
  [FLIGHT_MONITOR_CONTRACT.md](../SADE_AWS_API_INFORMATION/FLIGHT_MONITOR_CONTRACT.md)
  for sweeper timings.

- **CSV output is off by default.** `MISSION_ROWS_OUT=""` in the
  Dockerfile defaults disables local CSV writing — production output is
  the SADE finalize POST, not the CSV. Leave it disabled unless you
  mount an EFS volume and explicitly want diagnostic rows.

- **Logging.** Container logs are structured (timestamp + level +
  logger name + message). Suggested CloudWatch alarms:
  - Container exit (task stopped non-zero) → page oncall
  - `sessions_past_deadline > 0` for >15 min → SADE outbox or our
    sweeper isn't behaving
  - `sessions_stranded > 0` for >15 min → drones losing connectivity
    without SADE closing the session
  - `POST .../tracker-session-finalized → HTTP 5xx` rate spike → SADE
    finalize endpoint degraded

---

## 8. Cross-references

- [README.md](../README.md) — env-var contract, mTLS config, local dev
- [MESSAGE_SHAPES.md](MESSAGE_SHAPES.md) — wire-level message shapes for every I/O surface
- [FLIGHT_MONITOR_CONTRACT.md](../SADE_AWS_API_INFORMATION/FLIGHT_MONITOR_CONTRACT.md) — implementation-side contract (status codes, retry semantics, sweeper)
- [SADE_CONTRACT.md](../SADE_AWS_API_INFORMATION/SADE_CONTRACT.md) — authoritative payload shapes (SADE-owned)
- [scripts/run_stress_test.py](../scripts/run_stress_test.py) — load harness for sizing
- [docker-compose.yml](../docker-compose.yml) — local-stack reference (NOT for production)
