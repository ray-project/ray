import sys
from typing import AsyncGenerator, Generator

import pytest

import ray

from ray import serve
from ray.serve import Deployment
from ray.serve.handle import RayServeHandle
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_NEW_ROUTING,
)


@serve.deployment
class AsyncStreamer:
    async def __call__(
        self, n: int, should_error: bool = False
    ) -> AsyncGenerator[int, None]:
        if should_error:
            raise RuntimeError("oopsies")

        for i in range(n):
            yield i

    async def other_method(self, n: int) -> AsyncGenerator[int, None]:
        for i in range(n):
            yield i

    async def call_inner_generator(self, n: int) -> AsyncGenerator[int, None]:
        return self.other_method(n)

    async def unary(self, n: int) -> int:
        return n


@serve.deployment
class SyncStreamer:
    def __call__(
        self, n: int, should_error: bool = False
    ) -> Generator[int, None, None]:
        if should_error:
            raise RuntimeError("oopsies")

        for i in range(n):
            yield i

    def other_method(self, n: int) -> Generator[int, None, None]:
        for i in range(n):
            yield i

    def call_inner_generator(self, n: int) -> Generator[int, None, None]:
        return self.other_method(n)

    def unary(self, n: int) -> int:
        return n


@serve.deployment
def sync_gen_function(n: int):
    for i in range(n):
        yield i


@serve.deployment
async def async_gen_function(n: int):
    for i in range(n):
        yield i


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_NEW_ROUTING, reason="Routing FF must be enabled."
)
@pytest.mark.parametrize("deployment", [AsyncStreamer, SyncStreamer])
class TestAppHandleStreaming:
    def test_basic(self, serve_instance, deployment: Deployment):
        h = serve.run(deployment.bind()).options(stream=True)

        # Test calling __call__ generator.
        obj_ref_gen = ray.get(h.remote(5))
        assert ray.get(list(obj_ref_gen)) == list(range(5))

        # Test calling another method name.
        obj_ref_gen = ray.get(h.other_method.remote(5))
        assert ray.get(list(obj_ref_gen)) == list(range(5))

        # Test calling another method name via `.options`.
        obj_ref_gen = ray.get(h.options(method_name="other_method").remote(5))
        assert ray.get(list(obj_ref_gen)) == list(range(5))

        # Test calling a method that returns another generator.
        obj_ref_gen = ray.get(h.call_inner_generator.remote(5))
        assert ray.get(list(obj_ref_gen)) == list(range(5))

        # Test calling a unary method on the same deployment.
        assert ray.get(h.options(stream=False).unary.remote(5)) == 5

    def test_call_gen_without_stream_flag(self, serve_instance, deployment: Deployment):
        h = serve.run(deployment.bind())

        with pytest.raises(
            TypeError,
            match=(
                "Method '__call__' is a generator function. You must use "
                "`handle.options\(stream=True\)` to call generators on a deployment."
            ),
        ):
            ray.get(h.remote(5))

        with pytest.raises(
            TypeError,
            match=(
                "Method 'call_inner_generator' returned a generator. You must use "
                "`handle.options\(stream=True\)` to call generators on a deployment."
            ),
        ):
            ray.get(h.call_inner_generator.remote(5))

    def test_call_no_gen_with_stream_flag(self, serve_instance, deployment: Deployment):
        h = serve.run(deployment.bind()).options(stream=True)

        obj_ref_gen = ray.get(h.unary.remote(0))
        with pytest.raises(
            TypeError,
            match="must be a generator function, but 'unary' is not",
        ):
            ray.get(next(obj_ref_gen))

    def test_generator_yields_no_results(self, serve_instance, deployment: Deployment):
        h = serve.run(deployment.bind()).options(stream=True)

        obj_ref_gen = ray.get(h.remote(0))
        with pytest.raises(StopIteration):
            ray.get(next(obj_ref_gen))

    def test_exception_raised_in_gen(self, serve_instance, deployment: Deployment):
        h = serve.run(deployment.bind()).options(stream=True)

        obj_ref_gen = ray.get(h.remote(0, should_error=True))
        with pytest.raises(RuntimeError, match="oopsies"):
            ray.get(next(obj_ref_gen))


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_NEW_ROUTING, reason="Routing FF must be enabled."
)
@pytest.mark.parametrize("deployment", [AsyncStreamer, SyncStreamer])
class TestDeploymentHandleStreaming:
    def test_basic(self, serve_instance, deployment: Deployment):
        @serve.deployment
        class Delegate:
            def __init__(self, streamer: RayServeHandle):
                self._h = streamer

            async def __call__(self):
                h = self._h.options(stream=True)

                # Test calling __call__ generator.
                obj_ref_gen = await h.remote(5)
                assert [await obj_ref async for obj_ref in obj_ref_gen] == list(
                    range(5)
                )

                # Test calling another method name.
                obj_ref_gen = await h.other_method.remote(5)
                assert [await obj_ref for obj_ref in obj_ref_gen] == list(range(5))

                # Test calling another method name via `.options`.
                obj_ref_gen = await h.options(method_name="other_method").remote(5)
                assert [await obj_ref for obj_ref in obj_ref_gen] == list(range(5))

                # Test calling a unary method on the same deployment.
                assert await (await h.options(stream=False).unary.remote(5)) == 5

        h = serve.run(Delegate.bind(deployment.bind()))
        ray.get(h.remote())

    def test_call_gen_without_stream_flag(self, serve_instance, deployment: Deployment):
        @serve.deployment
        class Delegate:
            def __init__(self, streamer: RayServeHandle):
                self._h = streamer

            async def __call__(self):
                with pytest.raises(
                    TypeError,
                    match=(
                        "Method '__call__' is a generator function. You must use "
                        "`handle.options\(stream=True\)` to call generators on a "
                        "deployment."
                    ),
                ):
                    await (await self._h.remote(5))

                with pytest.raises(
                    TypeError,
                    match=(
                        "Method 'call_inner_generator' returned a generator. You must "
                        "use `handle.options\(stream=True\)` to call generators on a "
                        "deployment."
                    ),
                ):
                    await (await self._h.call_inner_generator.remote(5))

        h = serve.run(Delegate.bind(deployment.bind()))
        ray.get(h.remote())

    def test_call_no_gen_with_stream_flag(self, serve_instance, deployment: Deployment):
        @serve.deployment
        class Delegate:
            def __init__(self, streamer: RayServeHandle):
                self._h = streamer

            async def __call__(self):
                h = self._h.options(stream=True)

                obj_ref_gen = await h.unary.remote(0)
                with pytest.raises(
                    TypeError, match="must be a generator function, but 'unary' is not"
                ):
                    await (await obj_ref_gen.__anext__())

        h = serve.run(Delegate.bind(deployment.bind()))
        ray.get(h.remote())

    def test_generator_yields_no_results(self, serve_instance, deployment: Deployment):
        @serve.deployment
        class Delegate:
            def __init__(self, streamer: RayServeHandle):
                self._h = streamer

            async def __call__(self):
                h = self._h.options(stream=True)

                obj_ref_gen = await h.remote(0)
                with pytest.raises(StopAsyncIteration):
                    await (await obj_ref_gen.__anext__())

        h = serve.run(Delegate.bind(deployment.bind()))
        ray.get(h.remote())

    def test_exception_raised_in_gen(self, serve_instance, deployment: Deployment):
        @serve.deployment
        class Delegate:
            def __init__(self, streamer: RayServeHandle):
                self._h = streamer

            async def __call__(self):
                h = self._h.options(stream=True)

                obj_ref_gen = await h.remote(0, should_error=True)
                with pytest.raises(RuntimeError, match="oopsies"):
                    await (await obj_ref_gen.__anext__())

        h = serve.run(Delegate.bind(deployment.bind()))
        ray.get(h.remote())

    def test_call_multiple_downstreams(self, serve_instance, deployment: Deployment):
        @serve.deployment
        class Delegate:
            def __init__(self, streamer1: RayServeHandle, streamer2: RayServeHandle):
                self._h1 = streamer1.options(stream=True)
                self._h2 = streamer2.options(stream=True)

            async def __call__(self):
                obj_ref_gen1 = await self._h1.remote(1)
                obj_ref_gen2 = await self._h2.remote(2)

                assert await (await obj_ref_gen1.__anext__()) == 0
                assert await (await obj_ref_gen2.__anext__()) == 0

                with pytest.raises(StopAsyncIteration):
                    assert await (await obj_ref_gen1.__anext__())
                assert await (await obj_ref_gen2.__anext__()) == 1

                with pytest.raises(StopAsyncIteration):
                    assert await (await obj_ref_gen1.__anext__())
                with pytest.raises(StopAsyncIteration):
                    assert await (await obj_ref_gen2.__anext__())

        h = serve.run(Delegate.bind(deployment.bind(), deployment.bind()))
        ray.get(h.remote())


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_NEW_ROUTING, reason="Routing FF must be enabled."
)
@pytest.mark.parametrize("deployment", [sync_gen_function, async_gen_function])
class TestGeneratorFunctionDeployment:
    def test_app_handle(self, deployment: Deployment):
        h = serve.run(deployment.bind()).options(stream=True)
        obj_ref_gen = h.remote(5)
        assert ray.get(list(obj_ref_gen)) == list(range(5))

    def test_deployment_handle(self, deployment: Deployment):
        @serve.deployment
        class Delegate:
            def __init__(self, f: RayServeHandle):
                self._f = f.options(stream=True)

            async def __call__(self):
                obj_ref_gen = await self._f.remote(5)

                results = []
                async for obj_ref in obj_ref_gen:
                    results.append(await obj_ref)

                assert results == list(range(5))

        h = serve.run(Delegate.bind(deployment.bind()))
        ray.get(h.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
