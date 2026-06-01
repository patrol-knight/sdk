
import asyncio
import uvicorn

from typing import Any
from fastapi import FastAPI, Request

from sdk.core import ServiceTransport, RequestCallback


class RestServerTransport(ServiceTransport):
    def __init__(self, host: str = "0.0.0.0", port: int = 8000) -> None:
        self.host = host
        self.port = port

        self.app = FastAPI()
        self.server: uvicorn.Server | None = None
        self._server_task: asyncio.Task[None] | None = None
        self._started = False

    async def start(self) -> None:
        if self._started:
            return

        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            log_level="info",
        )

        self.server = uvicorn.Server(config)
        self._server_task = asyncio.create_task(self.server.serve())
        self._started = True

    async def close(self) -> None:
        if self.server is not None:
            self.server.should_exit = True

        if self._server_task is not None:
            await self._server_task

        self.server = None
        self._server_task = None
        self._started = False

    async def register_callback(
        self,
        endpoint: str,
        callback: RequestCallback,
    ) -> None:
        if self._started:
            raise RuntimeError("Cannot register callback after server has started")

        async def route(request: Request) -> Any:
            payload = await request.json()
            return await callback(payload)

        self.app.post(endpoint)(route)
