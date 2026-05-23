import pytest

from sdk.transports import MockTransport


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
async def test_mock_transport_requires_start_before_publish() -> None:
    transport = MockTransport()

    with pytest.raises(RuntimeError):
        await transport.publish("/test", {"hello": "world"})


@pytest.mark.asyncio
async def test_mock_transport_close_clears_subscribers() -> None:
    transport = MockTransport()
    received = []

    async def callback(topic: str, msg: object) -> None:
        received.append((topic, msg))

    await transport.start()
    await transport.subscribe("/test", callback)
    await transport.close()

    assert transport.closed is True
    assert transport.started is False
