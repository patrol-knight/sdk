from typing import Any
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

MessageCallback = Callable[[str, Any], Awaitable[None]]


class Transport(ABC):
    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def publish(self, topic: str, msg: Any) -> None: ...

    @abstractmethod
    async def subscribe(self, topic: str, callback: MessageCallback) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...
