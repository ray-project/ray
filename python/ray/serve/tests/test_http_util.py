import asyncio
import pickle
import pytest
from typing import Generator, Tuple, Union

from starlette.types import Message

import ray
from ray.actor import ActorHandle
from ray._private.utils import get_or_create_event_loop

from ray.serve._private.http_util import ASGIMessageQueue, ASGIReceiveProxy


@pytest.fixture(scope="session")
def shared_ray_instance(request):
    yield ray.init(num_cpus=16)
    ray.shutdown()


@pytest.mark.asyncio
async def test_asgi_message_queue():
    send = ASGIMessageQueue()

    # Check that wait_for_message hangs until a message is sent.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(send.wait_for_message(), 0.001)

    assert len(list(send.get_messages_nowait())) == 0

    await send({"type": "http.response.start"})
    await send.wait_for_message()
    assert len(list(send.get_messages_nowait())) == 1

    # Check that messages are cleared after being consumed.
    assert len(list(send.get_messages_nowait())) == 0
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(send.wait_for_message(), 0.001)

    # Check that consecutive messages are returned in order.
    await send({"type": "http.response.start", "idx": 0})
    await send({"type": "http.response.start", "idx": 1})
    await send.wait_for_message()
    messages = list(send.get_messages_nowait())
    assert len(messages) == 2
    assert messages[0]["idx"] == 0
    assert messages[1]["idx"] == 1

    assert len(list(send.get_messages_nowait())) == 0
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(send.wait_for_message(), 0.001)

    # Check that a concurrent waiter is notified when a message is available.
    loop = asyncio.get_running_loop()
    waiting_task = loop.create_task(send.wait_for_message())
    for _ in range(1000):
        assert not waiting_task.done()

    await send({"type": "http.response.start"})
    await waiting_task
    assert len(list(send.get_messages_nowait())) == 1


@pytest.fixture
def setup_receive_proxy(
    shared_ray_instance,
) -> Generator[Tuple[ASGIReceiveProxy, ActorHandle], None, None]:
    @ray.remote
    class ASGIReceive:
        def __init__(self):
            self._message_queue = ASGIMessageQueue()

        def ready(self):
            pass

        async def put(self, message: Union[Exception, Message]):
            await self._message_queue(message)

        async def receive_asgi_messages(self, request_id: str) -> bytes:
            await self._message_queue.wait_for_message()
            messages = self._message_queue.get_messages_nowait()
            for message in messages:
                if isinstance(message, Exception):
                    raise message

            return pickle.dumps(messages)

    actor = ASGIReceive.remote()
    ray.get(actor.ready.remote())
    loop = get_or_create_event_loop()
    asgi_receive_proxy = ASGIReceiveProxy(loop, "", actor)
    asgi_receive_proxy.start()
    try:
        yield asgi_receive_proxy, actor
    except Exception:
        asgi_receive_proxy.stop()


@pytest.mark.asyncio
class TestASGIReceiveProxy:
    async def test_basic(
        self, setup_receive_proxy: Tuple[ASGIReceiveProxy, ActorHandle]
    ):
        asgi_receive_proxy, actor = setup_receive_proxy

        await actor.put.remote({"type": "foo"})
        await actor.put.remote({"type": "bar"})
        assert await asgi_receive_proxy() == {"type": "foo"}
        assert await asgi_receive_proxy() == {"type": "bar"}

        assert asgi_receive_proxy._queue.empty()

        # Once disconnect is received, it should be returned repeatedly.
        await actor.put.remote({"type": "http.disconnect"})
        for _ in range(100):
            assert await asgi_receive_proxy() == {"type": "http.disconnect"}

        # Subsequent messages should be ignored.
        await actor.put.remote({"type": "baz"})
        assert await asgi_receive_proxy() == {"type": "http.disconnect"}

    async def test_actor_raises_exception(
        self, setup_receive_proxy: Tuple[ASGIReceiveProxy, ActorHandle]
    ):
        asgi_receive_proxy, actor = setup_receive_proxy

        await actor.put.remote({"type": "foo"})
        await actor.put.remote({"type": "bar"})
        await actor.put.remote(RuntimeError("oopsies"))
        assert await asgi_receive_proxy() == {"type": "foo"}
        assert await asgi_receive_proxy() == {"type": "bar"}

        with pytest.raises(RuntimeError, match="oopsies"):
            await asgi_receive_proxy()

    async def test_actor_crashes(
        self, setup_receive_proxy: Tuple[ASGIReceiveProxy, ActorHandle]
    ):
        asgi_receive_proxy, actor = setup_receive_proxy

        await actor.put.remote({"type": "foo"})
        await actor.put.remote({"type": "bar"})
        assert await asgi_receive_proxy() == {"type": "foo"}
        assert await asgi_receive_proxy() == {"type": "bar"}

        ray.kill(actor)
        with pytest.raises(ray.exceptions.RayActorError):
            await asgi_receive_proxy()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
