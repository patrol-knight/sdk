from typing import Any
from collections import defaultdict

from sdk.core.transport import Transport, MessageCallback


class MockTransport(Transport):
    def __init__(self) -> None:
        super().__init__()

        self.started: bool = False
        self.closed: bool  = False

        self._subs: dict[str, list[MessageCallback]] = defaultdict(list)

    async def start(self) -> None:
        self.started = True

    async def publish(self, topic: str, msg: Any) -> None:
        if not self.started:
            raise RuntimeError("Transport has not been started")
        
        for callback in self._subs.get(topic, []):
            await callback(topic, msg)

    async def subscribe(self, topic: str, callback: MessageCallback) -> None:
        if not self.started:
            raise RuntimeError("Transport has not been started")

        self._subs[topic].append(callback)

    async def close(self) -> None:
        self.closed  = True
        self.started = False
        self._subs.clear()
