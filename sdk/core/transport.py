from abc import ABC
from typing import Any
from collections.abc import Awaitable, Callable


MessageCallback = Callable[[str, Any], Awaitable[None]]
RequestCallback = Callable[[Any], Awaitable[Any]]


class Transport(ABC):
    async def start(self) -> None: ...

    async def close(self) -> None: ...


class EventTransport(Transport):
    async def publish(self, topic: str, msg: Any) -> None:
        raise NotImplementedError
    
    async def subscribe(
        self,
        topic: str,
        callback: MessageCallback
    ) -> None:
        raise NotImplementedError
    

class RequestTransport(Transport):
    async def get(self, endpoint: str, **kwargs: Any) -> Any:
        raise NotImplementedError
    
    async def post(self, endpoint: str, data: Any, **kwargs: Any) -> Any:
        raise NotImplementedError
    

class ServiceTransport(Transport):
    async def register_callback(
        self,
        endpoint: str,
        callback: RequestCallback,
    ) -> None:
        raise NotImplementedError
    
    async def serve(self) -> None:
        raise NotImplementedError
