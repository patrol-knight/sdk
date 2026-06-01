import logging
import asyncio

from abc import ABC, abstractmethod


class BaseWorker(ABC):
    def __init__(self, name: str = "worker") -> None:
        self.name = name
        self.running: bool = False
        self.logger = logging.getLogger(name)

    async def initialize(self) -> None:
        pass

    @abstractmethod
    async def process(self, data):
        raise NotImplementedError
    
    async def shutdown(self) -> None:
        pass

    async def on_error(self, exc: Exception) -> None:
        self.logger.exception(f"Worker error: {exc}")

    @abstractmethod
    async def _run(self) -> None:
        raise NotImplementedError
    
    def stop(self) -> None:
        self.running = False

    def run(self) -> None:
        asyncio.run(self._run)
