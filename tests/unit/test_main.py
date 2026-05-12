"""Unit tests for top-level service-orchestration helpers in app.main."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

from app.main import _make_pipeline_done_handler


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
