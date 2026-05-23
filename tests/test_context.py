import pytest

from sdk.core import WorkerContext
from sdk.transports import MockTransport


@pytest.mark.asyncio
async def test_context_publish_updates_latest() -> None:
    transport = MockTransport()
    ctx = WorkerContext(transport)

    await transport.start()
    await ctx.publish("/test", {"value": 1})

    assert ctx.get_latest("/test") == {"value": 1}


def test_context_set_latest() -> None:
    transport = MockTransport()
    ctx = WorkerContext(transport)

    ctx.set_latest("/test", {"value": 2})

    assert ctx.get_latest("/test") == {"value": 2}


def test_context_clear() -> None:
    transport = MockTransport()
    ctx = WorkerContext(transport)

    ctx.set_latest("/test", {"value": 3})
    ctx.clear()

    assert ctx.get_latest("/test") is None
