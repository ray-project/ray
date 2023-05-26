import asyncio
import pytest

from ray.serve._private.http_util import ASGIHTTPQueueSender


@pytest.mark.asyncio
async def test_asgi_queue_sender():
    sender = ASGIHTTPQueueSender()

    # Check that wait_for_message hangs until a message is sent.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(sender.wait_for_message(), 0.001)

    assert len(list(sender.get_messages_nowait())) == 0

    await sender({"type": "http.response.start"})
    await sender.wait_for_message()
    assert len(list(sender.get_messages_nowait())) == 1

    # Check that messages are cleared after being consumed.
    assert len(list(sender.get_messages_nowait())) == 0
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(sender.wait_for_message(), 0.001)

    # Check that consecutive messages are returned in order.
    await sender({"type": "http.response.start", "idx": 0})
    await sender({"type": "http.response.start", "idx": 1})
    await sender.wait_for_message()
    messages = list(sender.get_messages_nowait())
    assert len(messages) == 2
    assert messages[0]["idx"] == 0
    assert messages[1]["idx"] == 1

    assert len(list(sender.get_messages_nowait())) == 0
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(sender.wait_for_message(), 0.001)

    # Check that a concurrent waiter is notified when a message is available.
    loop = asyncio.get_running_loop()
    waiting_task = loop.create_task(sender.wait_for_message())
    for _ in range(1000):
        assert not waiting_task.done()

    await sender({"type": "http.response.start"})
    await waiting_task
    assert len(list(sender.get_messages_nowait())) == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
