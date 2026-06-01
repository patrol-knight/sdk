import httpx

from typing import Any, Optional
from sdk.core import RequestTransport


class ClientTransport(RequestTransport):
    def __init__(self, host: str, timeout: float = 5.0) -> None:
        self.host = host
        self.timeout = timeout
        self.client: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        self.client = httpx.AsyncClient(timeout=self.timeout)

    async def close(self) -> None:
        if self.client is not None:
            await self.client.aclose()
            self.client = None

    async def get(self, endpoint: str, **kwargs: Any):
        if self.client is None:
            raise RuntimeError("Client has not been stared")
        
        response = await self.client.get(
            f"{self.host}/{endpoint.lstrip('/')}",
            params=kwargs.get("params")
        )
        response.raise_for_status()
        return response.json()
    
    async def post(self, endpoint: str, data: Any, **kwargs: Any) -> Any:
        if self.client is None:
            raise RuntimeError("Client has not been started")
        
        response = await self.client.post(
            f"{self.host}/{endpoint.lstrip('/')}",
            json=data
        )
        response.raise_for_status()

        if not response.content:
            return None
        
        return response.json()
