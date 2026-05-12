# ── Build stage ──────────────────────────────────────────────────────────────
# Use slim to keep the image small. Pin the minor version so builds are
# reproducible; update deliberately when upgrading Python.
FROM python:3.12-slim

# Keeps Python from buffering stdout/stderr so logs appear immediately in
# CloudWatch / docker logs without needing PYTHONUNBUFFERED tricks in code.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /sade

# ── Dependencies ──────────────────────────────────────────────────────────────
# Copy requirements first so this layer is cached across code-only rebuilds.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Application code ──────────────────────────────────────────────────────────
COPY app/     ./app/
COPY run.py   .

# ── Runtime configuration defaults ───────────────────────────────────────────
# These can all be overridden at `docker run` time via -e / --env-file.
# See the README for the full list of supported environment variables.

# MQTT broker — required for non-local deployments; point at your broker host.
ENV MQTT_BROKER_HOST=localhost \
    MQTT_BROKER_PORT=1883 \
    MQTT_TOPIC=status_message,update_drone \
    MQTT_TLS_ENABLED=false
# MQTT_USERNAME and MQTT_PASSWORD are intentionally NOT set here.
# Pass them at runtime via -e or --env-file to avoid baking credentials
# into the image layer history.

# Pipeline behaviour
ENV SESSION_SOURCE_MODE=aws \
    FINALIZE_TO_API=true \
    WORKER_COUNT=1 \
    QUEUE_SIZE=10000 \
    SHUTDOWN_TIMEOUT=10 \
    IDLE_WARNING_SECONDS=300 \
    METRICS_LOG_INTERVAL=30 \
    LOG_LEVEL=INFO

# TRACKER_FINALIZED_URL is REQUIRED when FINALIZE_TO_API=true and is
# intentionally NOT given a default here.  Every deployment must pass its
# target SADE /tracker-session-finalized URL explicitly (via `docker run -e`,
# ECS task definition env, etc.) so the same image can be promoted across
# environments without a code change and so misconfiguration fails fast at
# pipeline startup rather than silently POSTing to the wrong backend.

# ── mTLS paths (AWS IoT Core) ────────────────────────────────────────────────
# These are *paths*, not contents.  The actual certificate + key files must be
# supplied at runtime via a bind mount:
#     docker run -v /host/sade-certs:/certs:ro ...
# or via ECS task definition secrets / AWS Secrets Manager.  NEVER `COPY` a
# private key (*.key) into this image — it would end up cached in the image
# layer history forever and be recoverable by anyone who has the image.
#
# All three paths must be set together for mTLS to engage.  Leaving them
# unset while MQTT_TLS_ENABLED=true uses generic TLS with the system CA
# bundle instead (for username/password-over-TLS brokers like HiveMQ Cloud).
ENV MQTT_CA_CERT_PATH=/certs/CAs.crt \
    MQTT_CLIENT_CERT_PATH=/certs/client.crt \
    MQTT_PRIVATE_KEY_PATH=/certs/client.key

# ── MQTT client identifier (AWS IoT Core) ────────────────────────────────────
# MQTT_CLIENT_ID is required when running against AWS IoT Core: the IoT policy
# attached to your client certificate typically restricts which client IDs the
# cert may use, so paho's random default will be silently rejected during the
# MQTT CONNECT phase (the connection appears to hang with no error logged).
#
# Intentionally NOT given a default here — each deployment must declare its
# own stable, policy-allowed identifier (e.g. `tlohman-flight-monitor`).  Only
# one MQTT connection may use a given ID at a time; a second connection with
# the same ID will cause IoT Core to disconnect the first.

# CSV output — disabled by default in the container (no persistent volume
# mounted). Set MISSION_ROWS_OUT to a path inside a mounted volume to enable.
ENV MISSION_ROWS_OUT=""

# FastAPI webhook server
ENV API_HOST=0.0.0.0 \
    API_PORT=8000

# ── Inbound API mTLS ─────────────────────────────────────────────────────────
# All three of API_CA_CERT_PATH / API_SERVER_CERT_PATH / API_SERVER_KEY_PATH
# must be set together to enable inbound mTLS on the FastAPI endpoints;
# leaving all three unset serves plain HTTP.  Intentionally NOT given
# defaults here — every deployment that wants mTLS must set them
# explicitly so we don't silently fall back to plain HTTP when a bind
# mount is misconfigured.  Mount cert files via:
#     docker run -v /host/api-certs:/api-certs:ro ...
# or via ECS task definition secrets / AWS Secrets Manager.  NEVER COPY
# the private key into the image — it would persist in the layer
# history forever.

# ── Outbound mTLS to SADE ────────────────────────────────────────────────────
# The Flight Monitor's identity cert (API_SERVER_CERT_PATH +
# API_SERVER_KEY_PATH above) is reused as the client cert presented to
# SADE on outbound /tracker-session-finalized POSTs — one "systems" cert,
# one place to configure it.  TRACKER_CA_CERT_PATH is independent: it's
# the CA used to verify SADE's SERVER cert during the outbound
# handshake.  Leave unset to use the system trust store (correct for
# publicly-trusted SADE endpoints).  Set when SADE uses an internal CA.

# ── Expose ────────────────────────────────────────────────────────────────────
# FastAPI webhook server:
#   POST /flight-monitor/register-session  (session registration)
#   POST /flight-monitor/exit-request      (exit notification)
#   GET  /health                           (liveness check)
#   GET  /dashboard                        (live drone-status web page)
# Same port whether mTLS is on or off — schema differs (http vs https).
EXPOSE 8000

# ── Entrypoint ────────────────────────────────────────────────────────────────
# run.py calls app.main.main() which starts both the FastAPI server and the
# MQTT pipeline in the same asyncio event loop.
CMD ["python", "run.py"]
