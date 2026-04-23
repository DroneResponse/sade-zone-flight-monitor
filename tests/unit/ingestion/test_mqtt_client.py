"""Unit tests for TelemetryMqttIngestionClient TLS / mTLS mode selection.

These tests monkeypatch ``paho.mqtt.client.Client`` so construction returns a
``MagicMock`` and we can capture the exact ``tls_set()`` invocations made by
``_build_client`` without needing a running broker, real certs, or network
access.

The four cases map one-to-one to the mode-selection rules documented on
``TelemetryMqttIngestionClient.__init__``.
"""

from __future__ import annotations

import asyncio
import ssl
from unittest.mock import MagicMock

import pytest

from app.ingestion import mqtt_client as mqtt_client_module
from app.ingestion.mqtt_client import TelemetryMqttIngestionClient


@pytest.fixture
def fake_paho(monkeypatch):
    """Replace paho's Client factory with a MagicMock per test.

    Returns the mock so individual tests can assert on ``tls_set`` calls.
    """
    fake_client_instance = MagicMock(name="paho_client")
    fake_client_factory = MagicMock(name="paho_client_factory", return_value=fake_client_instance)
    monkeypatch.setattr(mqtt_client_module.mqtt, "Client", fake_client_factory)
    # Expose the factory on the returned instance so tests can assert on how
    # ``mqtt.Client(...)`` itself was called (e.g. client_id forwarding).
    fake_client_instance._factory = fake_client_factory
    return fake_client_instance


def _make_client(**kwargs) -> TelemetryMqttIngestionClient:
    """Build an ingestion client with a fresh asyncio.Queue.

    Constructing the queue inside the test keeps the fixture noise down and
    doesn't require an event loop to be running for the tests we care about.
    """
    queue: asyncio.Queue = asyncio.Queue()
    return TelemetryMqttIngestionClient(queue, **kwargs)


class TestTlsModeSelection:
    def test_plain_tcp_does_not_call_tls_set(self, fake_paho):
        client = _make_client(tls_enabled=False)
        client._build_client()

        fake_paho.tls_set.assert_not_called()

    def test_mtls_calls_tls_set_with_all_three_paths(self, fake_paho):
        client = _make_client(
            tls_enabled=True,
            ca_cert_path="/certs/CAs.crt",
            client_cert_path="/certs/client.crt",
            private_key_path="/certs/client.key",
        )
        client._build_client()

        fake_paho.tls_set.assert_called_once_with(
            ca_certs="/certs/CAs.crt",
            certfile="/certs/client.crt",
            keyfile="/certs/client.key",
            tls_version=ssl.PROTOCOL_TLSv1_2,
        )

    def test_generic_tls_calls_tls_set_with_no_args(self, fake_paho):
        client = _make_client(tls_enabled=True)
        client._build_client()

        fake_paho.tls_set.assert_called_once_with()

    def test_partial_mtls_config_raises(self, fake_paho):
        client = _make_client(
            tls_enabled=True,
            ca_cert_path="/certs/CAs.crt",
            client_cert_path=None,
            private_key_path=None,
        )
        with pytest.raises(RuntimeError, match="mTLS configuration is incomplete"):
            client._build_client()

        # tls_set must not fire when we reject a partial config — otherwise we
        # would have silently downgraded to server-auth-only TLS.
        fake_paho.tls_set.assert_not_called()

    def test_partial_mtls_two_of_three_also_raises(self, fake_paho):
        client = _make_client(
            tls_enabled=True,
            ca_cert_path="/certs/CAs.crt",
            client_cert_path="/certs/client.crt",
            private_key_path=None,
        )
        with pytest.raises(RuntimeError, match="mTLS configuration is incomplete"):
            client._build_client()

        fake_paho.tls_set.assert_not_called()


class TestClientIdForwarding:
    def test_client_id_forwarded_to_paho(self, fake_paho):
        """When client_id is set, it's forwarded as a kwarg to mqtt.Client()."""
        client = _make_client(client_id="tlohman-flight-monitor")
        client._build_client()

        # Grab the call args to the factory.  Paho accepts client_id as kwarg.
        _args, kwargs = fake_paho._factory.call_args
        assert kwargs.get("client_id") == "tlohman-flight-monitor"

    def test_no_client_id_omits_kwarg(self, fake_paho):
        """When client_id is None, the kwarg is not passed (paho picks random)."""
        client = _make_client(client_id=None)
        client._build_client()

        _args, kwargs = fake_paho._factory.call_args
        assert "client_id" not in kwargs
