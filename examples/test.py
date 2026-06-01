from typing import Any

from sdk.core.worker import BaseWorker
from sdk.transports.mock import MockTransport


class TestBase(BaseWorker):
    input_topic = "/input"
    output_topic = "/output"
    state_topic = "/state"

    def __init__(self, name: str = "test_worker") -> None:
        super().__init__(name=name)

        self.transport = MockTransport()
        self.latest_state: dict[str, Any] = {}

    async def request_state(self) -> dict[str, Any]:
        return self.latest_state

    async def update_state(self, key: str, value: Any) -> None:
        self.latest_state[key] = value
        await self.transport.publish(self.state_topic, self.latest_state)

    async def publish_result(self, result: Any) -> None:
        await self.transport.publish(self.output_topic, result)

    async def on_message(self, topic: str, msg: Any) -> None:
        result = await self.process(msg)

        if result is not None:
            await self.publish_result(result)

    async def _run(self) -> None:
        await self.transport.start()
        await self.initialize()

        try:
            await self.transport.subscribe(self.input_topic, self.on_message)
            await self.transport.publish(self.input_topic, {"value": 123})

        finally:
            await self.shutdown()
            await self.transport.close()


class TestWorker(TestBase):
    async def initialize(self) -> None:
        self.scale = 2
        await self.update_state("ready", True)

    async def process(self, data: Any) -> Any:
        state = await self.request_state()
        value = data["value"]

        return {
            "input": value,
            "output": value * self.scale,
            "worker_ready": state["ready"],
            "status": "ok",
        }


if __name__ == "__main__":
    worker = TestWorker()
    worker.run()
