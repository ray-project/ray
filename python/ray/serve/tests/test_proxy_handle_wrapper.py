import asyncio
from typing import Tuple

import pytest

from ray import serve
from ray._private.test_utils import SignalActor, async_wait_for_condition
from ray.serve._private.proxy_handle_wrapper import ProxyHandleWrapper


def disconnect_task_and_event() -> Tuple[asyncio.Event, asyncio.Task]:
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

            def other_method(self, name: str) -> str:
                return f"Hello {name} from other method!"

            def get_mmid(self, name: str) -> str:
                return f"Hello {name}: {serve.get_multiplexed_model_id()}!"

            def error(self, name: str):
                raise RuntimeError("oopsies")

        h = serve.run(D.bind()).options(
            stream=False,
            use_new_handle_api=True,
        )
        p = ProxyHandleWrapper(h)

        responses = [r async for r in p.stream_request("Alice")]
        assert len(responses) == 1
        assert set(responses) == {"Hello Alice!"}

        responses = [
            r async for r in p.stream_request("Alice", method_name="other_method")
        ]
        assert len(responses) == 1
        assert set(responses) == {"Hello Alice from other method!"}

        responses = [
            r
            async for r in p.stream_request(
                "Alice", method_name="get_mmid", multiplexed_model_id="fake_model_id"
            )
        ]
        assert len(responses) == 1
        assert set(responses) == {"Hello Alice: fake_model_id!"}

        with pytest.raises(RuntimeError, match="oopsies"):
            await p.stream_request("", method_name="error").__anext__()

    async def test_result_callback(self, serve_instance):
        @serve.deployment
        class D:
            def __call__(self, name: str) -> str:
                return f"Hello {name}!"

        h = serve.run(D.bind()).options(
            stream=False,
            use_new_handle_api=True,
        )

        def result_callback(result: str) -> str:
            return f"Callback called on: {result}"

        p = ProxyHandleWrapper(h, result_callback=result_callback)

        responses = [r async for r in p.stream_request("Alice")]
        assert len(responses) == 1
        assert set(responses) == {"Callback called on: Hello Alice!"}

    async def test_timeout_while_assigning(self, serve_instance):
        signal_actor = SignalActor.remote()

        @serve.deployment(max_concurrent_queries=1)
        class D:
            async def __call__(self, _: str) -> str:
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=False,
            use_new_handle_api=True,
        )
        h.remote("")

        async def one_waiter():
            return await signal_actor.cur_num_waiters.remote() == 1

        await async_wait_for_condition(one_waiter)

        p = ProxyHandleWrapper(h)

        with pytest.raises(TimeoutError):
            await p.stream_request("", timeout_s=0.1).__anext__()

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()

    async def test_timeout_while_executing(self, serve_instance):
        signal_actor = SignalActor.remote()

        @serve.deployment
        class D:
            async def __call__(self, _: str) -> str:
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=False,
            use_new_handle_api=True,
        )

        p = ProxyHandleWrapper(h)

        with pytest.raises(TimeoutError):
            await p.stream_request("", timeout_s=0.1).__anext__()

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()

    async def test_disconnect_while_assigning(self, serve_instance):
        signal_actor = SignalActor.remote()
        disconnect_event, disconnect_task = disconnect_task_and_event()

        @serve.deployment(max_concurrent_queries=1)
        class D:
            async def __call__(self, _: str) -> str:
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=False,
            use_new_handle_api=True,
        )
        h.remote("")

        async def one_waiter():
            return await signal_actor.cur_num_waiters.remote() == 1

        await async_wait_for_condition(one_waiter)

        p = ProxyHandleWrapper(h)
        gen = p.stream_request("", disconnected_task=disconnect_task)

        async def get_next():
            return await gen.__anext__()

        gen_next = asyncio.ensure_future(get_next())
        done, _ = await asyncio.wait([gen_next], timeout=0.1)
        assert len(done) == 0

        # Set the disconnect event, causing the disconnect task to finish.
        disconnect_event.set()
        with pytest.raises(asyncio.CancelledError):
            await gen_next

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()

    async def test_disconnect_while_executing(self, serve_instance):
        signal_actor = SignalActor.remote()
        disconnect_event, disconnect_task = disconnect_task_and_event()

        @serve.deployment
        class D:
            async def __call__(self, _: str) -> str:
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=False,
            use_new_handle_api=True,
        )

        p = ProxyHandleWrapper(h)
        gen = p.stream_request("", disconnected_task=disconnect_task)

        async def get_next():
            return await gen.__anext__()

        gen_next = asyncio.ensure_future(get_next())
        done, _ = await asyncio.wait([gen_next], timeout=0.1)
        assert len(done) == 0

        # Set the disconnect event, causing the disconnect task to finish.
        disconnect_event.set()
        with pytest.raises(asyncio.CancelledError):
            await gen_next

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()


@pytest.mark.asyncio
class TestStreaming:
    async def test_basic(self, serve_instance):
        @serve.deployment
        class D:
            def __call__(self, name: str) -> str:
                for _ in range(5):
                    yield f"Hello {name}!"

            def other_method(self, name: str) -> str:
                for _ in range(5):
                    yield f"Hello {name} from other method!"

            def get_mmid(self, name: str) -> str:
                for _ in range(5):
                    yield f"Hello {name}: {serve.get_multiplexed_model_id()}!"

            def error(self, name: str):
                yield f"Hello {name}!"
                raise RuntimeError("oopsies")

        h = serve.run(D.bind()).options(
            stream=True,
            use_new_handle_api=True,
        )
        p = ProxyHandleWrapper(h)

        responses = [r async for r in p.stream_request("Alice")]
        assert len(responses) == 5
        assert set(responses) == {"Hello Alice!"}

        responses = [
            r async for r in p.stream_request("Alice", method_name="other_method")
        ]
        assert len(responses) == 5
        assert set(responses) == {"Hello Alice from other method!"}

        responses = [
            r
            async for r in p.stream_request(
                "Alice", method_name="get_mmid", multiplexed_model_id="fake_model_id"
            )
        ]
        assert len(responses) == 5
        assert set(responses) == {"Hello Alice: fake_model_id!"}

        gen = p.stream_request("Alice", method_name="error")
        assert await gen.__anext__() == "Hello Alice!"
        with pytest.raises(RuntimeError, match="oopsies"):
            await gen.__anext__()

    async def test_result_callback(self, serve_instance):
        @serve.deployment
        class D:
            def __call__(self, name: str) -> str:
                for _ in range(5):
                    yield f"Hello {name}!"

        h = serve.run(D.bind()).options(
            stream=True,
            use_new_handle_api=True,
        )

        def result_callback(result: str) -> str:
            return f"Callback called on: {result}"

        p = ProxyHandleWrapper(h, result_callback=result_callback)

        responses = [r async for r in p.stream_request("Alice")]
        assert len(responses) == 5
        assert set(responses) == {"Callback called on: Hello Alice!"}

    async def test_timeout_while_assigning(self, serve_instance):
        signal_actor = SignalActor.remote()

        @serve.deployment(max_concurrent_queries=1)
        class D:
            async def __call__(self, _: str) -> str:
                yield "hi"
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=True,
            use_new_handle_api=True,
        )
        h.remote("")

        async def one_waiter():
            return await signal_actor.cur_num_waiters.remote() == 1

        await async_wait_for_condition(one_waiter)

        p = ProxyHandleWrapper(h)

        with pytest.raises(TimeoutError):
            await p.stream_request("", timeout_s=0.1).__anext__()

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()

    async def test_timeout_while_executing(self, serve_instance):
        signal_actor = SignalActor.remote()

        @serve.deployment
        class D:
            async def __call__(self, _: str) -> str:
                yield "hi"
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=True,
            use_new_handle_api=True,
        )

        p = ProxyHandleWrapper(h)

        gen = p.stream_request("", timeout_s=0.1)
        assert (await gen.__anext__()) == "hi"
        with pytest.raises(TimeoutError):
            await gen.__anext__()

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()

    async def test_disconnect_while_assigning(self, serve_instance):
        signal_actor = SignalActor.remote()
        disconnect_event, disconnect_task = disconnect_task_and_event()

        @serve.deployment(max_concurrent_queries=1)
        class D:
            async def __call__(self, _: str) -> str:
                yield "hi"
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=True,
            use_new_handle_api=True,
        )
        h.remote("")

        async def one_waiter():
            return await signal_actor.cur_num_waiters.remote() == 1

        await async_wait_for_condition(one_waiter)

        p = ProxyHandleWrapper(h)
        gen = p.stream_request("", disconnected_task=disconnect_task)

        async def get_next():
            return await gen.__anext__()

        gen_next = asyncio.ensure_future(get_next())
        done, _ = await asyncio.wait([gen_next], timeout=0.1)
        assert len(done) == 0

        # Set the disconnect event, causing the disconnect task to finish.
        disconnect_event.set()
        with pytest.raises(asyncio.CancelledError):
            await gen_next

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()

    async def test_disconnect_while_executing(self, serve_instance):
        signal_actor = SignalActor.remote()
        disconnect_event, disconnect_task = disconnect_task_and_event()

        @serve.deployment
        class D:
            async def __call__(self, _: str) -> str:
                yield "hi"
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=True,
            use_new_handle_api=True,
        )

        p = ProxyHandleWrapper(h)
        gen = p.stream_request("", disconnected_task=disconnect_task)
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

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()

    async def test_stop_checking_disconnected_event(self, serve_instance):
        signal_actor = SignalActor.remote()
        disconnect_event, disconnect_task = disconnect_task_and_event()
        stop_checking_disconnected_event = asyncio.Event()

        @serve.deployment
        class D:
            async def __call__(self, _: str) -> str:
                yield "hi"
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            stream=True,
            use_new_handle_api=True,
        )

        p = ProxyHandleWrapper(h)
        gen = p.stream_request(
            "",
            disconnected_task=disconnect_task,
            stop_checking_disconnected_event=stop_checking_disconnected_event,
        )
        assert (await gen.__anext__()) == "hi"

        stop_checking_disconnected_event.set()

        async def get_next():
            return await gen.__anext__()

        gen_next = asyncio.ensure_future(get_next())
        done, _ = await asyncio.wait([gen_next], timeout=0.1)
        assert len(done) == 0

        # Set the disconnect event, causing the disconnect task to finish.
        # However, the `stop_checking_disconnected_event` is also set so this
        # should still time out.
        disconnect_event.set()
        done, _ = await asyncio.wait([gen_next], timeout=0.1)
        assert len(done) == 0

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
