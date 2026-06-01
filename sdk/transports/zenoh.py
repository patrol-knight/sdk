import json
import zenoh
import logging
import asyncio

from typing import Any

from sdk.core import MessageCallback, EventTransport


class ZenohTransport(EventTransport):
    def __init__(
        self,
        config_path: str | None = None,
        encoding: str = "application/json",
    ) -> None:
        self.config_path = config_path
        self.encoding = encoding

        self.session: Any | None = None
        self._subscribers: list[Any] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._started = False

        self.logger = logging.getLogger("zenoh_transport")

    async def start(self) -> None:
        if self._started:
            return

        self._loop = asyncio.get_running_loop()

        if self.config_path is not None:
            config = zenoh.Config.from_file(self.config_path)
        else:
            config = zenoh.Config()

        self.session = zenoh.open(config)
        self._started = True

    async def publish(self, topic: str, msg: Any) -> None:
        self._ensure_started()
        payload = self._serialize(msg)
        self.session.put(
            topic,
            payload,
            encoding=zenoh.Encoding(self.encoding),
        )

    async def subscribe(self, topic: str, callback: MessageCallback) -> None:
        self._ensure_started()

        def zenoh_callback(sample: Any) -> None:
            try:
                decoded_msg = self._deserialize(sample.payload)
                received_topic = str(sample.key_expr)

                self._schedule_callback(
                    callback=callback,
                    topic=received_topic,
                    msg=decoded_msg,
                )

            except Exception as exc:
                self.logger.exception("Failed to handle Zenoh sample: %s", exc)

        subscriber = self.session.declare_subscriber(
            topic,
            zenoh_callback,
        )

        self._subscribers.append(subscriber)

    async def close(self) -> None:
        for subscriber in self._subscribers:
            try:
                subscriber.undeclare()
            except Exception as exc:
                self.logger.warning("Failed to undeclare subscriber: %s", exc)

        self._subscribers.clear()

        if self.session is not None:
            try:
                self.session.close()
            except Exception as exc:
                self.logger.warning("Failed to close Zenoh session: %s", exc)

        self.session = None
        self._loop = None
        self._started = False

    def _ensure_started(self) -> None:
        if not self._started or self.session is None:
            raise RuntimeError("ZenohTransport has not been started")

    def _schedule_callback(
        self,
        callback: MessageCallback,
        topic: str,
        msg: Any,
    ) -> None:
        if self._loop is None:
            raise RuntimeError("Asyncio event loop is not available")

        future = asyncio.run_coroutine_threadsafe(
            callback(topic, msg),
            self._loop,
        )

        def on_done(done_future: asyncio.Future[Any]) -> None:
            try:
                done_future.result()
            except Exception as exc:
                self.logger.exception("Zenoh subscriber callback failed: %s", exc)

        future.add_done_callback(on_done)

    def _serialize(self, msg: Any) -> bytes:
        return json.dumps(msg).encode("utf-8")

    def _deserialize(self, payload: Any) -> Any:
        if hasattr(payload, "to_bytes"):
            raw = payload.to_bytes()
        elif isinstance(payload, bytes):
            raw = payload
        else:
            raw = bytes(payload)

        return json.loads(raw.decode("utf-8"))
