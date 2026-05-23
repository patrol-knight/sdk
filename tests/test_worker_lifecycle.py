import pytest
import asyncio

from typing import Any

from sdk.core import BaseWorker, WorkerContext
from sdk.transports import MockTransport


class LifecycleWorker(BaseWorker):
    subscriptions = ["/test"]

    def __init__(self) -> None:
        super().__init__(name="lifecycle_worker", update_rate=100.0)

        self.initialized     = False
        self.processed       = False
        self.ticked          = False
        self.shutdown_called = False
        
        self.received_topic: str | None = None
        self.received_msg: Any | None = None

    async def initialize(self) -> None:
        self.initialized = True

    async def process(self, topic: str, msg: Any) -> None:
        self.processed = True
        self.received_topic = topic
        self.received_msg = msg
        self.stop()

    async def tick(self) -> None:
        self.ticked = True

    async def shutdown(self) -> None:
        self.shutdown_called = True


@pytest.mark.asyncio
async def test_worker_lifecycle_process_message() -> None:
    transport = MockTransport()
    ctx = WorkerContext(transport)
    worker = LifecycleWorker()

    await transport.start()

    task = asyncio.create_task(worker.start(ctx))

    await asyncio.sleep(0)
    await transport.publish("/test", {"value": 1})
    await task

    assert worker.initialized is True
    assert worker.processed is True
    assert worker.ticked is True
    assert worker.shutdown_called is True
    assert worker.received_topic == "/test"
    assert worker.received_msg == {"value": 1}
    assert ctx.get_latest("/test") == {"value": 1}


class ErrorWorker(BaseWorker):
    subscriptions = ["/test"]

    def __init__(self) -> None:
        super().__init__(name="error_worker", update_rate=100.0)
        self.error_handled = False
        self.shutdown_called = False

    async def process(self, topic: str, msg: Any) -> None:
        self.stop()
        raise RuntimeError("intentional error")

    async def on_error(self, exc: Exception) -> None:
        self.error_handled = True

    async def shutdown(self) -> None:
        self.shutdown_called = True


@pytest.mark.asyncio
async def test_worker_handles_process_error() -> None:
    transport = MockTransport()
    ctx = WorkerContext(transport)
    worker = ErrorWorker()

    await transport.start()

    task = asyncio.create_task(worker.start(ctx))

    await asyncio.sleep(0)
    await transport.publish("/test", {"value": 1})
    await task

    assert worker.error_handled is True
    assert worker.shutdown_called is True
