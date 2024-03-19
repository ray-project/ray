import asyncio
from typing import AsyncIterator, Tuple

import pytest

from ray import serve
from ray._private.test_utils import SignalActor, async_wait_for_condition
from ray.serve._private.proxy_response_generator import ProxyResponseGenerator


def disconnect_task_and_event() -> Tuple[asyncio.Event, asyncio.Task]:
    """Return an event and a task waiting on it for testing disconnect logic."""
    event = asyncio.Event()

    async def wait_for_event():
        return await event.wait()

    return event, asyncio.ensure_future(wait_for_event())


@pytest.mark.asyncio
class TestUnary:
    async def test_basic(self, serve_instance):
        @serve.deployment
        class D:
            def __call__(self, name: str) -> str:
                return f"Hello {name}!"

            def error(self):
                raise RuntimeError("oopsies")

        h = serve.run(D.bind()).options(
            stream=False,
        )
        gen = ProxyResponseGenerator(h.remote("Alice"))

        # Test simple response.
        responses = [r async for r in gen]
        assert len(responses) == 1
        assert set(responses) == {"Hello Alice!"}

        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

        # Test response raising exception.
        gen = ProxyResponseGenerator(h.error.remote())
        with pytest.raises(RuntimeError, match="oopsies"):
            await gen.__anext__()

        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    async def test_result_callback(self, serve_instance):
        @serve.deployment
        class D:
            def __call__(self, name: str) -> str:
                return f"Hello {name}!"

        h = serve.run(D.bind()).options(
            stream=False,
        )

        def result_callback(result: str) -> str:
            return f"Callback called on: {result}"

        gen = ProxyResponseGenerator(h.remote("Alice"), result_callback=result_callback)

        responses = [r async for r in gen]
        assert len(responses) == 1
        assert set(responses) == {"Callback called on: Hello Alice!"}

    async def test_timeout_while_assigning(self, serve_instance):
        signal_actor = SignalActor.remote()

        @serve.deployment(max_ongoing_requests=1)
        class D:
            async def __call__(self):
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=False,
        )
        h.remote()

        async def one_waiter():
            return await signal_actor.cur_num_waiters.remote() == 1

        await async_wait_for_condition(one_waiter)

        gen = ProxyResponseGenerator(h.remote(), timeout_s=0.1)

        with pytest.raises(TimeoutError):
            await gen.__anext__()

        assert gen.cancelled()

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()

    async def test_timeout_while_executing(self, serve_instance):
        signal_actor = SignalActor.remote()

        @serve.deployment
        class D:
            async def __call__(self):
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=False,
        )

        gen = ProxyResponseGenerator(h.remote(), timeout_s=0.1)

        with pytest.raises(TimeoutError):
            await gen.__anext__()

        assert gen.cancelled()

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()

    async def test_disconnect_while_assigning(self, serve_instance):
        signal_actor = SignalActor.remote()
        disconnect_event, disconnect_task = disconnect_task_and_event()

        @serve.deployment(max_ongoing_requests=1)
        class D:
            async def __call__(self):
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=False,
        )
        h.remote()

        async def one_waiter():
            return await signal_actor.cur_num_waiters.remote() == 1

        await async_wait_for_condition(one_waiter)

        gen = ProxyResponseGenerator(h.remote(), disconnected_task=disconnect_task)

        async def get_next():
            return await gen.__anext__()

        gen_next = asyncio.ensure_future(get_next())
        done, _ = await asyncio.wait([gen_next], timeout=0.1)
        assert len(done) == 0

        # Set the disconnect event, causing the disconnect task to finish.
        disconnect_event.set()
        with pytest.raises(asyncio.CancelledError):
            await gen_next

        assert gen.cancelled()

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()

    async def test_disconnect_while_executing(self, serve_instance):
        signal_actor = SignalActor.remote()
        disconnect_event, disconnect_task = disconnect_task_and_event()

        @serve.deployment
        class D:
            async def __call__(self):
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=False,
        )

        gen = ProxyResponseGenerator(h.remote(), disconnected_task=disconnect_task)

        async def get_next():
            return await gen.__anext__()

        gen_next = asyncio.ensure_future(get_next())
        done, _ = await asyncio.wait([gen_next], timeout=0.1)
        assert len(done) == 0

        # Set the disconnect event, causing the disconnect task to finish.
        disconnect_event.set()
        with pytest.raises(asyncio.CancelledError):
            await gen_next

        assert gen.cancelled()

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()


@pytest.mark.asyncio
class TestStreaming:
    async def test_basic(self, serve_instance):
        @serve.deployment
        class D:
            def __call__(self, name: str) -> AsyncIterator[str]:
                for _ in range(5):
                    yield f"Hello {name}!"

            def error(self, name: str) -> AsyncIterator[str]:
                yield f"Hello {name}!"
                raise RuntimeError("oopsies")

        h = serve.run(D.bind()).options(
            stream=True,
        )
        gen = ProxyResponseGenerator(h.remote("Alice"))

        # Test simple response.
        responses = [r async for r in gen]
        assert len(responses) == 5
        assert set(responses) == {"Hello Alice!"}
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

        # Test exception in the middle of response stream.
        gen = ProxyResponseGenerator(h.error.remote("Alice"))
        assert await gen.__anext__() == "Hello Alice!"
        with pytest.raises(RuntimeError, match="oopsies"):
            await gen.__anext__()
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    async def test_result_callback(self, serve_instance):
        @serve.deployment
        class D:
            def __call__(self, name: str) -> AsyncIterator[str]:
                for _ in range(5):
                    yield f"Hello {name}!"

        h = serve.run(D.bind()).options(
            stream=True,
        )

        def result_callback(result: str) -> str:
            return f"Callback called on: {result}"

        gen = ProxyResponseGenerator(h.remote("Alice"), result_callback=result_callback)

        responses = [r async for r in gen]
        assert len(responses) == 5
        assert set(responses) == {"Callback called on: Hello Alice!"}

    async def test_timeout_while_assigning(self, serve_instance):
        signal_actor = SignalActor.remote()

        @serve.deployment(max_ongoing_requests=1)
        class D:
            async def __call__(self) -> AsyncIterator[str]:
                yield "hi"
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=True,
        )
        h.remote()

        async def one_waiter():
            return await signal_actor.cur_num_waiters.remote() == 1

        await async_wait_for_condition(one_waiter)

        gen = ProxyResponseGenerator(h.remote(), timeout_s=0.1)

        with pytest.raises(TimeoutError):
            await gen.__anext__()

        assert gen.cancelled()

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()

    async def test_timeout_while_executing(self, serve_instance):
        signal_actor = SignalActor.remote()

        @serve.deployment
        class D:
            async def __call__(self) -> AsyncIterator[str]:
                yield "hi"
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=True,
        )

        gen = ProxyResponseGenerator(h.remote(), timeout_s=0.1)
        assert (await gen.__anext__()) == "hi"
        with pytest.raises(TimeoutError):
            await gen.__anext__()

        assert gen.cancelled()

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()

    async def test_disconnect_while_assigning(self, serve_instance):
        signal_actor = SignalActor.remote()
        disconnect_event, disconnect_task = disconnect_task_and_event()

        @serve.deployment(max_ongoing_requests=1)
        class D:
            async def __call__(self) -> AsyncIterator[str]:
                yield "hi"
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=True,
        )
        h.remote()

        async def one_waiter():
            return await signal_actor.cur_num_waiters.remote() == 1

        await async_wait_for_condition(one_waiter)

        gen = ProxyResponseGenerator(h.remote(), disconnected_task=disconnect_task)

        async def get_next():
            return await gen.__anext__()

        gen_next = asyncio.ensure_future(get_next())
        done, _ = await asyncio.wait([gen_next], timeout=0.1)
        assert len(done) == 0

        # Set the disconnect event, causing the disconnect task to finish.
        disconnect_event.set()
        with pytest.raises(asyncio.CancelledError):
            await gen_next

        assert gen.cancelled()

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()

    async def test_disconnect_while_executing(self, serve_instance):
        signal_actor = SignalActor.remote()
        disconnect_event, disconnect_task = disconnect_task_and_event()

        @serve.deployment
        class D:
            async def __call__(self) -> AsyncIterator[str]:
                yield "hi"
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=True,
        )

        gen = ProxyResponseGenerator(h.remote(), disconnected_task=disconnect_task)
        assert (await gen.__anext__()) == "hi"

        async def get_next():
            return await gen.__anext__()

        gen_next = asyncio.ensure_future(get_next())
        done, _ = await asyncio.wait([gen_next], timeout=0.1)
        assert len(done) == 0

        # Set the disconnect event, causing the disconnect task to finish.
        disconnect_event.set()
        with pytest.raises(asyncio.CancelledError):
            await gen_next

        assert gen.cancelled()

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()

    async def test_stop_checking_for_disconnect(self, serve_instance):
        signal_actor = SignalActor.remote()
        disconnect_event, disconnect_task = disconnect_task_and_event()

        @serve.deployment
        class D:
            async def __call__(self) -> AsyncIterator[str]:
                yield "hi"
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=True,
        )

        gen = ProxyResponseGenerator(
            h.remote(),
            disconnected_task=disconnect_task,
        )
        assert (await gen.__anext__()) == "hi"

        gen.stop_checking_for_disconnect()

        async def get_next():
            return await gen.__anext__()

        gen_next = asyncio.ensure_future(get_next())
        done, _ = await asyncio.wait([gen_next], timeout=0.1)
        assert len(done) == 0

        # Set the disconnect event, causing the disconnect task to finish.
        # However, because stop_checking_for_disconnect was called this
        # should still time out.
        disconnect_event.set()
        done, _ = await asyncio.wait([gen_next], timeout=0.1)
        assert len(done) == 0

        assert not gen.cancelled()

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
