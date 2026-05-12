"""Unit tests for top-level service-orchestration helpers in app.main."""

from __future__ import annotations

import asyncio
import ssl
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.main import _make_pipeline_done_handler, _resolve_api_tls_config


def _make_fake_task(*, cancelled: bool, exception: BaseException | None) -> Mock:
    """Build a Mock that mimics the asyncio.Task surface the handler reads."""
    task = Mock(spec=asyncio.Task)
    task.cancelled.return_value = cancelled
    task.exception.return_value = exception
    return task


def test_handler_signals_server_shutdown_when_pipeline_raises() -> None:
    """A crashed pipeline must flip ``server.should_exit`` so uvicorn stops.

    This is the whole point of the callback — a fire-and-forget pipeline
    task that crashes silently would otherwise leave ``/health`` green
    while no telemetry is being processed.
    """
    server = SimpleNamespace(should_exit=False)
    handler = _make_pipeline_done_handler(server)

    task = _make_fake_task(cancelled=False, exception=RuntimeError("pipeline boom"))
    handler(task)

    assert server.should_exit is True


def test_handler_signals_server_shutdown_when_pipeline_exits_cleanly() -> None:
    """``run_pipeline`` should never return on its own — treat it as an error.

    The pipeline awaits an event that never fires, so a normal return
    means something tore the loop down without cancelling us, which
    leaves uvicorn unable to source new telemetry.  Bring the server
    down instead of silently continuing.
    """
    server = SimpleNamespace(should_exit=False)
    handler = _make_pipeline_done_handler(server)

    task = _make_fake_task(cancelled=False, exception=None)
    handler(task)

    assert server.should_exit is True


def test_handler_ignores_cancellation() -> None:
    """Cancellation is the normal shutdown path — must NOT trigger a re-shutdown.

    ``run_service``'s ``finally`` block cancels the task as part of
    teardown.  The handler then fires with ``task.cancelled() == True``;
    flipping ``should_exit`` again is harmless but reading
    ``task.exception()`` on a cancelled task raises ``CancelledError``,
    so the early return matters for correctness too.
    """
    server = SimpleNamespace(should_exit=False)
    handler = _make_pipeline_done_handler(server)

    task = _make_fake_task(cancelled=True, exception=None)
    handler(task)

    assert server.should_exit is False
    task.exception.assert_not_called()


# ── _resolve_api_tls_config ──────────────────────────────────────────────────


def _make_args(*, ca: str = "", cert: str = "", key: str = "") -> SimpleNamespace:
    """Build the minimal args surface that ``_resolve_api_tls_config`` reads."""
    return SimpleNamespace(api_ca_cert=ca, api_server_cert=cert, api_server_key=key)


def test_tls_resolver_returns_none_when_all_three_unset() -> None:
    """All three unset is the documented 'plain HTTP' signal."""
    result = _resolve_api_tls_config(_make_args())
    assert result is None


def test_tls_resolver_returns_uvicorn_kwargs_when_all_three_set(tmp_path: Path) -> None:
    """Happy path: all three files exist → return a dict ready to splat into ``uvicorn.Config``."""
    ca = tmp_path / "ca.crt"
    cert = tmp_path / "server.crt"
    key = tmp_path / "server.key"
    for f in (ca, cert, key):
        f.write_text("placeholder")

    result = _resolve_api_tls_config(_make_args(ca=str(ca), cert=str(cert), key=str(key)))

    assert result == {
        "ssl_keyfile": str(key),
        "ssl_certfile": str(cert),
        "ssl_ca_certs": str(ca),
        "ssl_cert_reqs": ssl.CERT_REQUIRED,
    }


def test_tls_resolver_returns_none_when_only_cert_and_key_set(tmp_path: Path) -> None:
    """cert+key without ca is the 'outbound-only identity' config — no inbound mTLS.

    The same systems cert is reused as the outbound client cert to SADE
    (see resolve_outbound_tls_config in tracker_finalizer.py), so this
    combination must NOT raise — an operator who terminates inbound at
    an ALB and only needs outbound mTLS should be able to set just the
    two identity vars.
    """
    cert = tmp_path / "server.crt"
    key = tmp_path / "server.key"
    cert.write_text("x")
    key.write_text("x")

    result = _resolve_api_tls_config(_make_args(cert=str(cert), key=str(key)))

    assert result is None


def test_tls_resolver_raises_when_cert_without_key(tmp_path: Path) -> None:
    """Half an identity is always a misconfiguration regardless of inbound vs outbound."""
    cert = tmp_path / "server.crt"
    cert.write_text("x")

    with pytest.raises(RuntimeError, match="must be set together"):
        _resolve_api_tls_config(_make_args(cert=str(cert)))


def test_tls_resolver_raises_when_ca_without_identity(tmp_path: Path) -> None:
    """CA-only is useless without a server cert — almost always a typo."""
    ca = tmp_path / "ca.crt"
    ca.write_text("x")

    with pytest.raises(RuntimeError, match="all three"):
        _resolve_api_tls_config(_make_args(ca=str(ca)))


def test_tls_resolver_raises_when_file_missing(tmp_path: Path) -> None:
    """All three paths set but one file is absent — surface clearly at startup."""
    ca = tmp_path / "ca.crt"
    cert = tmp_path / "server.crt"
    ca.write_text("x")
    cert.write_text("x")
    nonexistent_key = tmp_path / "nope.key"

    with pytest.raises(RuntimeError, match="not found on disk"):
        _resolve_api_tls_config(_make_args(
            ca=str(ca), cert=str(cert), key=str(nonexistent_key),
        ))


def test_tls_resolver_strips_whitespace() -> None:
    """Whitespace-only env values are treated as unset (common docker-compose footgun)."""
    result = _resolve_api_tls_config(_make_args(ca="   ", cert="", key="\t"))
    assert result is None
