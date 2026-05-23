import asyncio
import logging

from typing import Any
from abc import ABC, abstractmethod

from sdk.core.context import WorkerContext


class BaseWorker(ABC):
    subscriptions: list[str] = []

    def __init__(
        self,
        name: str = "worker",
        update_rate: float = 10.0
    ) -> None:
        self.name = name
        self.update_rate = update_rate

        self.running: bool = False
        self.ctx: WorkerContext | None = None

        self.logger = logging.getLogger(name)

    async def initialize(self) -> None:
        pass

    @abstractmethod
    async def process(self, topic: str, msg: Any) -> None: ...

    async def tick(self) -> None: pass

    async def shutdown(self) -> None: pass

    async def start(self, ctx: WorkerContext) -> None:
        self.ctx = ctx
        self.running = True

        await self.initialize()

        for topic in self.subscriptions:
            await self.ctx.subscribe(topic, self._on_message)

        try:
            while self.running:
                await self.tick()
                await asyncio.sleep(1.0 / self.update_rate)
        
        finally:
            await self.shutdown()

    async def stop(self) -> None:
        self.running = False
    
    async def on_error(self, exce: Exception) -> None:
        self.logger.exception(exce)

    async def _on_message(self, topic: str, msg: Any) -> None:
        try:
            if self.ctx is not None:
                self.ctx.set_latest(topic, msg)
            
            await self.process(topic, msg)

        except Exception as exce:
            await self.on_error(exce)
