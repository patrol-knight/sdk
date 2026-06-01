import pytest

from sdk.transports import MockTransport


@pytest.mark.asyncio
async def test_mock_transport_start_and_close() -> None:
    transport = MockTransport()

    await transport.start()

    assert transport.started is True
    assert transport.closed is False

    await transport.close()

    assert transport.started is False
    assert transport.closed is True


@pytest.mark.asyncio
async def test_mock_transport_publish_to_subscriber() -> None:
    transport = MockTransport()
    received = []

    async def callback(topic: str, msg: object) -> None:
        received.append((topic, msg))

    await transport.start()
    await transport.subscribe("/test", callback)
    await transport.publish("/test", {"hello": "world"})

    assert received == [("/test", {"hello": "world"})]


@pytest.mark.asyncio
async def test_mock_transport_publish_to_multiple_subscribers() -> None:
    transport = MockTransport()
    received_1 = []
    received_2 = []

    async def callback_1(topic: str, msg: object) -> None:
        received_1.append((topic, msg))

    async def callback_2(topic: str, msg: object) -> None:
        received_2.append((topic, msg))

    await transport.start()
    await transport.subscribe("/test", callback_1)
    await transport.subscribe("/test", callback_2)

    await transport.publish("/test", 123)

    assert received_1 == [("/test", 123)]
    assert received_2 == [("/test", 123)]


@pytest.mark.asyncio
async def test_mock_transport_does_not_publish_to_other_topics() -> None:
    transport = MockTransport()
    received = []

    async def callback(topic: str, msg: object) -> None:
        received.append((topic, msg))

    await transport.start()
    await transport.subscribe("/target", callback)
    await transport.publish("/other", {"ignored": True})

    assert received == []


@pytest.mark.asyncio
async def test_mock_transport_requires_start_before_publish() -> None:
    transport = MockTransport()

    with pytest.raises(RuntimeError, match="not been started"):
        await transport.publish("/test", {"hello": "world"})


@pytest.mark.asyncio
async def test_mock_transport_requires_start_before_subscribe() -> None:
    transport = MockTransport()

    async def callback(topic: str, msg: object) -> None:
        pass

    with pytest.raises(RuntimeError, match="not been started"):
        await transport.subscribe("/test", callback)


@pytest.mark.asyncio
async def test_mock_transport_close_clears_subscribers() -> None:
    transport = MockTransport()
    received = []

    async def callback(topic: str, msg: object) -> None:
        received.append((topic, msg))

    await transport.start()
    await transport.subscribe("/test", callback)
    await transport.close()
    await transport.start()
    await transport.publish("/test", "message")

    assert received == []
