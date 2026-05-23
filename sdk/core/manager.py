import asyncio
import logging

from sdk.core.worker import BaseWorker
from sdk.core.transport import Transport
from sdk.core.context import WorkerContext


class WorkerManager(object):
    def __init__(self, worker: BaseWorker, transport: Transport) -> None:
        self.worker = worker
        self.transport = transport

        self.ctx = WorkerContext(transport)
        self.logger = logging.getLogger("worker_manager")

    async def start(self) -> None:
        self.logger.info("Starting Transport")
        await self.transport.start()

        self.logger.info("Starting Worker")
        await self.worker.start(self.ctx)

    async def shutdown(self) -> None:
        self.logger.info("Stopping Worker")
        self.worker.stop()

        self.logger.info("Closing Transport")
        await self.transport.close()

    def run(self) -> None:
        asyncio.run(self._run())

    async def _run(self) -> None:
        try:
            await self.start()
        
        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt Received")
        
        finally:
            await self.shutdown()
