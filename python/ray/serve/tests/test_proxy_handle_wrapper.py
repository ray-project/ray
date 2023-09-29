import asyncio
import time

import pytest

from ray import serve
from ray.serve.handle import DeploymentHandle
from ray.serve._private.proxy_handle_wrapper import ProxyHandleWrapper
from ray._private.test_utils import SignalActor, async_wait_for_condition

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
            use_new_handle_api=True,
        )
        p = ProxyHandleWrapper(h.options(stream=False))

        responses = [r async for r in p.stream_request("Alice")]
        assert len(responses) == 1
        assert set(responses) == {"Hello Alice!"}

        responses = [r async for r in p.stream_request("Alice", method_name="other_method")]
        assert len(responses) == 1
        assert set(responses) == {"Hello Alice from other method!"}

        responses = [r async for r in p.stream_request("Alice", method_name="get_mmid", multiplexed_model_id="fake_model_id")]
        assert len(responses) == 1
        assert set(responses) == {"Hello Alice: fake_model_id!"}

        with pytest.raises(RuntimeError, match="oopsies"):
            await p.stream_request("", method_name="error").__anext__()


    async def test_timeout_while_assigning(self, serve_instance):
        signal_actor = SignalActor.remote()

        @serve.deployment(max_concurrent_queries=1)
        class D:
            async def __call__(self, _: str) -> str:
                await signal_actor.wait.remote()

        h = serve.run(D.bind()).options(
            use_new_handle_api=True,
        )
        h.remote("")

        async def one_waiter():
            return await signal_actor.cur_num_waiters.remote() == 1

        await async_wait_for_condition(one_waiter)

        p = ProxyHandleWrapper(h.options(stream=False))

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
            use_new_handle_api=True,
        )

        p = ProxyHandleWrapper(h.options(stream=False))

        with pytest.raises(TimeoutError):
            await p.stream_request("", timeout_s=0.1).__anext__()

        assert await signal_actor.cur_num_waiters.remote() == 1
        await signal_actor.send.remote()


@pytest.mark.asyncio
class TestStreaming:
    async def test_basic_streaming(self):
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
            use_new_handle_api=True,
        )
        p = ProxyHandleWrapper(h.options(stream=True))

        responses = [r async for r in p.stream_request("Alice")]
        assert len(responses) == 5
        assert set(responses) == {"Hello Alice!"}

        responses = [r async for r in p.stream_request("Alice", method_name="other_method")]
        assert len(responses) == 5
        assert set(responses) == {"Hello Alice from other method!"}

        responses = [r async for r in p.stream_request("Alice", method_name="get_mmid", multiplexed_model_id="fake_model_id")]
        assert len(responses) == 5
        assert set(responses) == {"Hello Alice: fake_model_id!"}

        gen = p.stream_request("Alice", method_name="error")
        assert await gen.__anext__() == "Hello Alice!"
        with pytest.raises(RuntimeError, match="oopsies"):
            await gen.__anext__()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
