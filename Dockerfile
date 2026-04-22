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
    MQTT_TOPIC=update_drone \
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

# CSV output — disabled by default in the container (no persistent volume
# mounted). Set MISSION_ROWS_OUT to a path inside a mounted volume to enable.
ENV MISSION_ROWS_OUT=""

# FastAPI webhook server
ENV API_HOST=0.0.0.0 \
    API_PORT=8000

# ── Expose ────────────────────────────────────────────────────────────────────
# FastAPI webhook server:
#   POST /flight-monitor/register-session  (session registration)
#   POST /flight-monitor/exit-request      (exit notification)
#   GET  /health                           (liveness check)
EXPOSE 8000

# ── Entrypoint ────────────────────────────────────────────────────────────────
# run.py calls app.main.main() which starts both the FastAPI server and the
# MQTT pipeline in the same asyncio event loop.
CMD ["python", "run.py"]
