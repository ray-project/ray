import asyncio
import pickle
from typing import Generator, Tuple

import pytest

from ray._private.utils import get_or_create_event_loop
from ray.serve._private.http_util import ASGIReceiveProxy, MessageQueue


@pytest.mark.asyncio
async def test_message_queue():
    queue = MessageQueue()

    # Check that wait_for_message hangs until a message is sent.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(queue.wait_for_message(), 0.001)

    assert len(list(queue.get_messages_nowait())) == 0

    await queue({"type": "http.response.start"})
    await queue.wait_for_message()
    assert len(list(queue.get_messages_nowait())) == 1

    # Check that messages are cleared after being consumed.
    assert len(list(queue.get_messages_nowait())) == 0
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(queue.wait_for_message(), 0.001)

    # Check that consecutive messages are returned in order.
    await queue({"type": "http.response.start", "idx": 0})
    await queue({"type": "http.response.start", "idx": 1})
    await queue.wait_for_message()
    messages = list(queue.get_messages_nowait())
    assert len(messages) == 2
    assert messages[0]["idx"] == 0
    assert messages[1]["idx"] == 1

    assert len(list(queue.get_messages_nowait())) == 0
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(queue.wait_for_message(), 0.001)

    # Check that a concurrent waiter is notified when a message is available.
    loop = asyncio.get_running_loop()
    waiting_task = loop.create_task(queue.wait_for_message())
    for _ in range(1000):
        assert not waiting_task.done()

    await queue({"type": "http.response.start"})
    await waiting_task
    assert len(list(queue.get_messages_nowait())) == 1

    # Check that once the queue is closed, new messages should be rejected and
    # ongoing and subsequent calls to wait for messages should return immediately.
    waiting_task = loop.create_task(queue.wait_for_message())
    queue.close()
    await waiting_task  # Ongoing call should return.

    for _ in range(100):
        with pytest.raises(RuntimeError):
            await queue({"hello": "world"})
        await queue.wait_for_message()
        assert queue.get_messages_nowait() == []


@pytest.fixture
@pytest.mark.asyncio
def setup_receive_proxy() -> Generator[
    Tuple[ASGIReceiveProxy, MessageQueue], None, None
]:
    queue = MessageQueue()

    async def receive_asgi_messages(request_id: str) -> bytes:
        await queue.wait_for_message()
        messages = queue.get_messages_nowait()
        for message in messages:
            if isinstance(message, Exception):
                raise message

        return pickle.dumps(messages)

    loop = get_or_create_event_loop()
    asgi_receive_proxy = ASGIReceiveProxy("", receive_asgi_messages)
    receiver_task = loop.create_task(asgi_receive_proxy.fetch_until_disconnect())
    try:
        yield asgi_receive_proxy, queue
    except Exception:
        receiver_task.cancel()


@pytest.mark.asyncio
class TestASGIReceiveProxy:
    async def test_basic(
        self, setup_receive_proxy: Tuple[ASGIReceiveProxy, MessageQueue]
    ):
        asgi_receive_proxy, queue = setup_receive_proxy

        queue.put_nowait({"type": "foo"})
        queue.put_nowait({"type": "bar"})
        assert await asgi_receive_proxy() == {"type": "foo"}
        assert await asgi_receive_proxy() == {"type": "bar"}

        assert asgi_receive_proxy._queue.empty()

        # Once disconnect is received, it should be returned repeatedly.
        queue.put_nowait({"type": "http.disconnect"})
        for _ in range(100):
            assert await asgi_receive_proxy() == {"type": "http.disconnect"}

        # Subsequent messages should be ignored.
        queue.put_nowait({"type": "baz"})
        assert await asgi_receive_proxy() == {"type": "http.disconnect"}

    async def test_raises_exception(
        self, setup_receive_proxy: Tuple[ASGIReceiveProxy, MessageQueue]
    ):
        asgi_receive_proxy, queue = setup_receive_proxy

        queue.put_nowait({"type": "foo"})
        queue.put_nowait({"type": "bar"})
        assert await asgi_receive_proxy() == {"type": "foo"}
        assert await asgi_receive_proxy() == {"type": "bar"}

        queue.put_nowait(RuntimeError("oopsies"))
        with pytest.raises(RuntimeError, match="oopsies"):
            await asgi_receive_proxy()

    async def test_does_not_raise_key_error(
        self, setup_receive_proxy: Tuple[ASGIReceiveProxy, MessageQueue]
    ):
        asgi_receive_proxy, queue = setup_receive_proxy

        queue.put_nowait({"type": "foo"})
        queue.put_nowait({"type": "bar"})
        assert await asgi_receive_proxy() == {"type": "foo"}
        assert await asgi_receive_proxy() == {"type": "bar"}

        queue.put_nowait(KeyError("not found"))
        _, pending = await asyncio.wait(
            [asyncio.create_task(asgi_receive_proxy())], timeout=0.01
        )
        assert len(pending) == 1

    async def test_receive_asgi_messages_raises(self):
        async def receive_asgi_messages(request_id: str) -> bytes:
            raise RuntimeError("maybe actor crashed")

        loop = get_or_create_event_loop()
        asgi_receive_proxy = ASGIReceiveProxy("", receive_asgi_messages)
        receiver_task = loop.create_task(asgi_receive_proxy.fetch_until_disconnect())

        try:
            with pytest.raises(RuntimeError, match="maybe actor crashed"):
                await asgi_receive_proxy()
        finally:
            receiver_task.cancel()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
