from typing import Any

from sdk.core.transport import MessageCallback, Transport


class WorkerContext(object):
    def __init__(self, transport: Transport) -> None:
        self.transport = transport
        self._latest: dict[str, Any] = {}

    async def publish(self, topic: str, msg: Any) -> None:
        self._latest[topic] = msg
        await self.transport.publish(topic, msg)

    async def subscribe(self, topic: str, callback: MessageCallback) -> None:
        await self.transport.subscribe(topic, callback)

    def set_latest(self, topic: str, msg: Any) -> None:
        self._latest[topic] = msg

    def get_latest(self, topic: str) -> Any | None:
        return self._latest.get(topic)
    
    def clear(self) -> None:
        self._latest.clear()
