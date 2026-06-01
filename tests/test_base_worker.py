import pytest

from sdk.core import BaseWorker


class MockWorker(BaseWorker):
    def __init__(self) -> None:
        super().__init__(name="test_worker")
        self.initialized = False
        self.shutdown_called = False
        self.processed = []

    async def initialize(self) -> None:
        self.initialized = True

    async def process(self, data):
        self.processed.append(data)
        return data

    async def shutdown(self) -> None:
        self.shutdown_called = True

    async def _run(self) -> None:
        await self.initialize()
        try:
            await self.process({"hello": "world"})
        finally:
            await self.shutdown()


@pytest.mark.asyncio
async def test_base_worker_lifecycle() -> None:
    worker = MockWorker()

    await worker._run()

    assert worker.initialized is True
    assert worker.processed == [{"hello": "world"}]
    assert worker.shutdown_called is True


class ErrorWorker(BaseWorker):
    def __init__(self) -> None:
        super().__init__(name="error_worker")
        self.error_handled = False

    async def process(self, data):
        raise RuntimeError("process failed")

    async def on_error(self, exc: Exception) -> None:
        self.error_handled = True

    async def _run(self) -> None:
        try:
            await self.process({"bad": True})
        except Exception as exc:
            await self.on_error(exc)


@pytest.mark.asyncio
async def test_base_worker_on_error() -> None:
    worker = ErrorWorker()

    await worker._run()

    assert worker.error_handled is True
