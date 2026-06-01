from typing import Any, Dict
from collections import defaultdict

from sdk.core import MessageCallback, EventTransport


class MockTransport(EventTransport):
    def __init__(self) -> None:
        self.started: bool = False
        self.closed:  bool = False

        self._sub: Dict[str, list[MessageCallback]] = defaultdict(list)

    async def start(self) -> None:
        self.started = True
        self.closed = False

    async def publish(self, topic: str, msg: Any) -> None:
        if not self.started:
            raise RuntimeError("Transport has not been started")
        
        for callback in self._sub.get(topic, []):
            await callback(topic, msg)

    async def subscribe(self, topic: str, callback: MessageCallback) -> None:
        if not self.started:
            raise RuntimeError("Transport has not been started")
        
        self._sub[topic].append(callback)

    async def close(self) -> None:
        self.closed  = True
        self.started = False
        self._sub.clear()
