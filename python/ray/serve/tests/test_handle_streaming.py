import sys
from typing import AsyncGenerator, Generator

import pytest

from ray import serve
from ray.serve import Deployment
from ray.serve.handle import DeploymentHandle


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


@pytest.mark.parametrize("deployment", [AsyncStreamer, SyncStreamer])
class TestAppHandleStreaming:
    def test_basic(self, serve_instance, deployment: Deployment):
        h = serve.run(deployment.bind()).options(stream=True)

        # Test calling __call__ generator.
        gen = h.remote(5)
        assert list(gen) == list(range(5))

        # Test calling another method name.
        gen = h.other_method.remote(5)
        assert list(gen) == list(range(5))

        # Test calling another method name via `.options`.
        gen = h.options(method_name="other_method").remote(5)
        assert list(gen) == list(range(5))

        # Test calling a method that returns another generator.
        gen = h.call_inner_generator.remote(5)
        assert list(gen) == list(range(5))

        # Test calling a unary method on the same deployment.
        assert h.options(stream=False).unary.remote(5).result() == 5

    def test_call_gen_without_stream_flag(self, serve_instance, deployment: Deployment):
        h = serve.run(deployment.bind())

        with pytest.raises(
            TypeError,
            match=(
                r"Method '__call__' returned a generator. You must use "
                r"`handle.options\(stream=True\)` to call generators on a deployment."
            ),
        ):
            h.remote(5).result()

        with pytest.raises(
            TypeError,
            match=(
                r"Method 'call_inner_generator' returned a generator. You must use "
                r"`handle.options\(stream=True\)` to call generators on a deployment."
            ),
        ):
            h.call_inner_generator.remote(5).result()

    def test_call_no_gen_with_stream_flag(self, serve_instance, deployment: Deployment):
        h = serve.run(deployment.bind()).options(stream=True)

        gen = h.unary.remote(0)
        with pytest.raises(
            TypeError,
            match=r"'unary' .* but it did not return a generator",
        ):
            next(gen)

    def test_generator_yields_no_results(self, serve_instance, deployment: Deployment):
        h = serve.run(deployment.bind()).options(stream=True)

        gen = h.remote(0)
        with pytest.raises(StopIteration):
            next(gen)

    def test_exception_raised_in_gen(self, serve_instance, deployment: Deployment):
        h = serve.run(deployment.bind()).options(stream=True)

        gen = h.remote(0, should_error=True)
        with pytest.raises(RuntimeError, match="oopsies"):
            next(gen)


@pytest.mark.parametrize("deployment", [AsyncStreamer, SyncStreamer])
class TestDeploymentHandleStreaming:
    def test_basic(self, serve_instance, deployment: Deployment):
        @serve.deployment
        class Delegate:
            def __init__(self, streamer: DeploymentHandle):
                self._h = streamer

            async def __call__(self):
                h = self._h.options(stream=True)

                # Test calling __call__ generator.
                gen = h.remote(5)
                assert [result async for result in gen] == list(range(5))

                # Test calling another method name.
                gen = h.other_method.remote(5)
                assert [result async for result in gen] == list(range(5))

                # Test calling another method name via `.options`.
                gen = h.options(method_name="other_method").remote(5)
                assert [result async for result in gen] == list(range(5))

                # Test calling a unary method on the same deployment.
                assert await h.options(stream=False).unary.remote(5) == 5

        h = serve.run(Delegate.bind(deployment.bind()))
        h.remote().result()

    def test_call_gen_without_stream_flag(self, serve_instance, deployment: Deployment):
        @serve.deployment
        class Delegate:
            def __init__(self, streamer: DeploymentHandle):
                self._h = streamer

            async def __call__(self):
                with pytest.raises(
                    TypeError,
                    match=(
                        r"Method '__call__' returned a generator. You must use "
                        r"`handle.options\(stream=True\)` to call generators on a "
                        r"deployment."
                    ),
                ):
                    await self._h.remote(5)

                with pytest.raises(
                    TypeError,
                    match=(
                        r"Method 'call_inner_generator' returned a generator. You must "
                        r"use `handle.options\(stream=True\)` to call generators on a "
                        r"deployment."
                    ),
                ):
                    await self._h.call_inner_generator.remote(5)

        h = serve.run(Delegate.bind(deployment.bind()))
        h.remote().result()

    def test_call_no_gen_with_stream_flag(self, serve_instance, deployment: Deployment):
        @serve.deployment
        class Delegate:
            def __init__(self, streamer: DeploymentHandle):
                self._h = streamer

            async def __call__(self):
                h = self._h.options(stream=True)

                gen = h.unary.remote(0)
                with pytest.raises(
                    TypeError,
                    match=r"'unary' .* but it did not return a generator",
                ):
                    await gen.__anext__()

        h = serve.run(Delegate.bind(deployment.bind()))
        h.remote().result()

    def test_generator_yields_no_results(self, serve_instance, deployment: Deployment):
        @serve.deployment
        class Delegate:
            def __init__(self, streamer: DeploymentHandle):
                self._h = streamer

            async def __call__(self):
                h = self._h.options(stream=True)

                gen = h.remote(0)
                with pytest.raises(StopAsyncIteration):
                    await gen.__anext__()

        h = serve.run(Delegate.bind(deployment.bind()))
        h.remote().result()

    def test_exception_raised_in_gen(self, serve_instance, deployment: Deployment):
        @serve.deployment
        class Delegate:
            def __init__(self, streamer: DeploymentHandle):
                self._h = streamer

            async def __call__(self):
                h = self._h.options(stream=True)

                gen = h.remote(0, should_error=True)
                with pytest.raises(RuntimeError, match="oopsies"):
                    await gen.__anext__()

        h = serve.run(Delegate.bind(deployment.bind()))
        h.remote().result()

    def test_call_multiple_downstreams(self, serve_instance, deployment: Deployment):
        @serve.deployment
        class Delegate:
            def __init__(
                self, streamer1: DeploymentHandle, streamer2: DeploymentHandle
            ):
                self._h1 = streamer1.options(stream=True)
                self._h2 = streamer2.options(stream=True)

            async def __call__(self):
                gen1 = self._h1.remote(1)
                gen2 = self._h2.remote(2)

                assert await gen1.__anext__() == 0
                assert await gen2.__anext__() == 0

                with pytest.raises(StopAsyncIteration):
                    assert await gen1.__anext__()
                assert await gen2.__anext__() == 1

                with pytest.raises(StopAsyncIteration):
                    assert await gen1.__anext__()
                with pytest.raises(StopAsyncIteration):
                    assert await gen2.__anext__()

        h = serve.run(
            Delegate.bind(deployment.bind(), deployment.bind()),
        )
        h.remote().result()


@pytest.mark.parametrize("deployment", [sync_gen_function, async_gen_function])
class TestGeneratorFunctionDeployment:
    def test_app_handle(self, deployment: Deployment):
        h = serve.run(deployment.bind()).options(stream=True)
        gen = h.remote(5)
        assert list(gen) == list(range(5))

    def test_deployment_handle(self, deployment: Deployment):
        @serve.deployment
        class Delegate:
            def __init__(self, f: DeploymentHandle):
                self._f = f.options(stream=True)

            async def __call__(self):
                gen = self._f.remote(5)
                assert [result async for result in gen] == list(range(5))

        h = serve.run(Delegate.bind(deployment.bind()))
        h.remote().result()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
