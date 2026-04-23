"""Async MQTT ingestion client for drone telemetry.

This module provides a lightweight MQTT subscriber that only ingests messages
and forwards them to a shared ``asyncio.Queue``. It intentionally avoids any
telemetry parsing or business logic in MQTT callbacks.

Design goals:
- Keep MQTT callbacks fast and non-blocking.
- Bridge safely from Paho's callback thread to an asyncio event loop.
- Hand off all downstream work to async workers via a queue.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
import time
from datetime import datetime, timezone
from typing import Any, Optional

from app.monitoring.pipeline_metrics import PipelineMetrics

import paho.mqtt.client as mqtt

LOGGER = logging.getLogger(__name__)


def _reason_code_to_int(reason_code: Any) -> int:
    """Convert Paho reason-code objects and integers into a plain int."""
    if reason_code is None:
        return 0

    value = getattr(reason_code, "value", reason_code)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0 if str(reason_code) == "Success" else 1


class TelemetryMqttIngestionClient:
    """MQTT ingestion client that pushes telemetry events into an asyncio queue.

    The queue is dependency-injected from the outside so this module stays
    focused on transport/integration only.
    """

    def __init__(
        self,
        queue: asyncio.Queue[dict[str, Any]],
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        broker: str = "localhost",
        port: int = 1883,
        topic: str = "update_drone",
        keepalive: int = 30,
        metrics: Optional[PipelineMetrics] = None,
        # Auth / TLS — required for non-local brokers (e.g. AWS IoT Core, HiveMQ Cloud).
        username: Optional[str] = None,
        password: Optional[str] = None,
        tls_enabled: bool = False,
        # mTLS — required for AWS IoT Core.  All three paths must be set
        # together, or all three must be left unset.  See ``_build_client``
        # for the mode-selection rules.
        ca_cert_path: Optional[str] = None,
        client_cert_path: Optional[str] = None,
        private_key_path: Optional[str] = None,
        # MQTT client ID — AWS IoT Core policies typically restrict which
        # client IDs a given cert can use, so this MUST be set for IoT Core
        # deployments.  When None, paho generates a random client ID (fine
        # for local / unauthenticated brokers).
        client_id: Optional[str] = None,
    ) -> None:
        """Initialize ingestion client configuration.

        Args:
            queue: Shared asyncio queue for incoming telemetry envelopes.
            loop: Event loop that owns ``queue``. If omitted, uses the running
                loop at start time.
            broker: MQTT broker host.
            port: MQTT broker port.
            topic: MQTT telemetry topic or wildcard topic filter.
            keepalive: MQTT keepalive interval in seconds.
            username: Optional MQTT username for broker authentication.
            password: Optional MQTT password (used only when username is set).
            tls_enabled: When True, enables TLS on the connection. Required for
                most cloud brokers.
            ca_cert_path: Path to the server-authentication CA certificate.
                Required for mTLS (AWS IoT Core).
            client_cert_path: Path to this client's signed X.509 certificate.
                Required for mTLS (AWS IoT Core).
            private_key_path: Path to this client's private key (matches
                ``client_cert_path``).  Required for mTLS (AWS IoT Core).

        TLS / mTLS mode selection (evaluated in ``_build_client``):
            * ``tls_enabled=False`` → plain TCP.  All three cert paths are
              ignored.  Intended for localhost / development.
            * ``tls_enabled=True`` and all three cert paths set → mTLS.
              paho is configured with ``ca_certs`` + ``certfile`` + ``keyfile``
              using TLSv1.2.  This is the AWS IoT Core path.
            * ``tls_enabled=True`` and none of the three cert paths set →
              generic TLS using the system CA bundle.  Supports managed
              brokers that use username/password over TLS (e.g. HiveMQ Cloud).
            * ``tls_enabled=True`` with one or two of the three paths set →
              raise ``RuntimeError``.  A partial mTLS config is almost
              always a misconfiguration rather than intent.
        """
        self.queue = queue
        self.loop = loop
        self.broker = broker
        self.port = port
        self.topic = topic
        self.keepalive = keepalive
        self.metrics = metrics
        self.username = username
        self.password = password
        self.tls_enabled = tls_enabled
        self.ca_cert_path = ca_cert_path
        self.client_cert_path = client_cert_path
        self.private_key_path = private_key_path
        self.client_id = client_id

        self._client: Optional[mqtt.Client] = None
        self._last_message_monotonic: float | None = None

    def start(self) -> mqtt.Client:
        """Connect to broker, subscribe to telemetry topic, and start MQTT loop.

        Returns:
            The configured Paho MQTT client instance.
        """
        # Capture the event loop that owns the shared asyncio queue.
        if self.loop is None:
            self.loop = asyncio.get_running_loop()

        client = self._build_client()
        self._client = client

        LOGGER.info("Connecting to MQTT broker %s:%s", self.broker, self.port)
        client.connect(self.broker, self.port, keepalive=self.keepalive)

        # Start Paho network handling in a background thread managed by Paho.
        client.loop_start()
        return client

    def stop(self) -> None:
        """Stop MQTT network loop and disconnect from the broker."""
        if self._client is None:
            return

        try:
            self._client.loop_stop()
            self._client.disconnect()
            LOGGER.info("MQTT client stopped")
        except Exception:  # noqa: BLE001
            LOGGER.exception("Error while stopping MQTT client")

    def seconds_since_last_message(self) -> float | None:
        """Return idle seconds since the most recently received MQTT message."""
        if self._last_message_monotonic is None:
            return None
        return max(0.0, time.monotonic() - self._last_message_monotonic)

    def _build_client(self) -> mqtt.Client:
        """Create and configure a Paho MQTT client with callbacks.

        Uses callback API v2 when available, while staying compatible with older
        Paho versions.
        """
        # Pass client_id only when explicitly configured.  None lets paho
        # generate a random ID (fine for local / unauthenticated brokers) but
        # AWS IoT Core policies typically restrict which client IDs a given
        # cert is allowed to use, so real deployments must set this.
        client_id_kwarg = {"client_id": self.client_id} if self.client_id else {}
        try:
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, **client_id_kwarg)
        except Exception:  # noqa: BLE001
            client = mqtt.Client(**client_id_kwarg)

        # Configure TLS / mTLS before setting credentials — order matters for Paho.
        self._configure_tls(client)

        # Set credentials if provided. Leave unset for unauthenticated brokers.
        if self.username:
            client.username_pw_set(self.username, self.password or "")

        # Keep callbacks minimal: connect/subscribe and enqueue only.
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        return client

    def _configure_tls(self, client: mqtt.Client) -> None:
        """Apply the correct TLS / mTLS mode to the paho client.

        Decides between three modes based on the instance's TLS attributes:

        1. Plain TCP — ``tls_enabled`` is False.  No ``tls_set()`` call.
        2. mTLS — ``tls_enabled`` is True and all three cert paths are set.
           ``tls_set()`` is called with explicit ``ca_certs`` + ``certfile``
           + ``keyfile`` paths.  This is the AWS IoT Core mode.
        3. Generic TLS — ``tls_enabled`` is True and no cert paths are set.
           ``tls_set()`` is called with no args so paho uses the system
           trust store.  Useful for username/password-over-TLS brokers.

        Raises:
            RuntimeError: ``tls_enabled`` is True and one or two of the three
                cert paths are set (a partial configuration).  Almost always a
                misconfiguration rather than intent, so we fail fast at
                pipeline startup rather than silently skipping client auth.
        """
        if not self.tls_enabled:
            LOGGER.info("MQTT transport: plain TCP (tls_enabled=False)")
            return

        cert_paths = (self.ca_cert_path, self.client_cert_path, self.private_key_path)
        set_count = sum(1 for path in cert_paths if path)

        if set_count == 3:
            client.tls_set(
                ca_certs=self.ca_cert_path,
                certfile=self.client_cert_path,
                keyfile=self.private_key_path,
                tls_version=ssl.PROTOCOL_TLSv1_2,
            )
            LOGGER.info(
                "MQTT transport: mTLS (ca=%s cert=%s key=%s)",
                self.ca_cert_path,
                self.client_cert_path,
                self.private_key_path,
            )
            return

        if set_count == 0:
            client.tls_set()
            LOGGER.info("MQTT transport: generic TLS (system CA bundle)")
            return

        raise RuntimeError(
            "MQTT mTLS configuration is incomplete. "
            "Set all three of MQTT_CA_CERT_PATH, MQTT_CLIENT_CERT_PATH, and "
            "MQTT_PRIVATE_KEY_PATH for AWS IoT Core, or leave all three unset "
            "for generic TLS using the system CA bundle. "
            f"Currently set: ca_cert_path={bool(self.ca_cert_path)}, "
            f"client_cert_path={bool(self.client_cert_path)}, "
            f"private_key_path={bool(self.private_key_path)}."
        )

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: dict[str, Any],
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        """Subscribe to telemetry topic after a successful broker connection."""
        _ = userdata, flags, properties

        rc = _reason_code_to_int(reason_code)
        if rc != 0:
            LOGGER.error("MQTT connect failed with rc=%s", reason_code)
            return

        LOGGER.info("Connected to MQTT broker (rc=%s)", rc)
        client.subscribe(self.topic, qos=0)
        LOGGER.info("Subscribed to topic: %s", self.topic)

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        """Receive MQTT message and enqueue a normalized message envelope.

        This callback may run on Paho's network thread, so we schedule queue
        insertion onto the asyncio loop thread via ``call_soon_threadsafe``.
        """
        _ = client, userdata

        if self.loop is None:
            LOGGER.error("Event loop is not set; dropping message from topic %s", msg.topic)
            return

        # Track the most recent inbound telemetry event for idle monitoring.
        self._last_message_monotonic = time.monotonic()

        # Normalize payload to text so downstream workers have a consistent shape.
        payload = msg.payload
        if isinstance(payload, (bytes, bytearray)):
            payload_text = payload.decode("utf-8", errors="replace")
        else:
            payload_text = str(payload)

        # Envelope contains only transport metadata + raw payload.
        message_envelope: dict[str, Any] = {
            "topic": msg.topic,
            "payload": payload_text,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            # The monotonic timestamp lets workers measure queue wait time
            # without depending on wall-clock synchronization.
            "enqueued_monotonic": time.monotonic(),
        }

        # Thread-safe hop from Paho callback thread -> asyncio event loop thread.
        self.loop.call_soon_threadsafe(self._enqueue_message, message_envelope)

    def _enqueue_message(self, message_envelope: dict[str, Any]) -> None:
        """Insert telemetry envelope into queue without blocking the loop."""
        try:
            # Non-blocking queue write keeps ingestion path lightweight.
            self.queue.put_nowait(message_envelope)
            if self.metrics is not None:
                self.metrics.record_enqueue(self.queue.qsize())
            LOGGER.debug("Telemetry enqueued from topic: %s", message_envelope["topic"])
        except asyncio.QueueFull:
            # If bounded queue is full, drop safely and record the event.
            if self.metrics is not None:
                self.metrics.record_drop(self.queue.qsize())
            LOGGER.warning(
                "Telemetry queue full; dropping message from topic: %s",
                message_envelope["topic"],
            )


def start_mqtt_client(
    queue: asyncio.Queue[dict[str, Any]],
    *,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    broker: str = "localhost",
    port: int = 1883,
    topic: str = "update_drone",
    keepalive: int = 30,
    metrics: Optional[PipelineMetrics] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    tls_enabled: bool = False,
    ca_cert_path: Optional[str] = None,
    client_cert_path: Optional[str] = None,
    private_key_path: Optional[str] = None,
    client_id: Optional[str] = None,
) -> TelemetryMqttIngestionClient:
    """Convenience helper to create and start the ingestion MQTT client.

    Accepts the same auth / TLS / mTLS kwargs as
    ``TelemetryMqttIngestionClient.__init__`` and forwards them verbatim.
    See that class's docstring for mode-selection rules.
    """
    client = TelemetryMqttIngestionClient(
        queue,
        loop=loop,
        broker=broker,
        port=port,
        topic=topic,
        keepalive=keepalive,
        metrics=metrics,
        username=username,
        password=password,
        tls_enabled=tls_enabled,
        ca_cert_path=ca_cert_path,
        client_cert_path=client_cert_path,
        private_key_path=private_key_path,
        client_id=client_id,
    )
    client.start()
    return client
