import asyncio
import os
from typing import AsyncGenerator

import pytest
import requests
from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import StreamingResponse

import ray
from ray import serve
from ray._private.test_utils import SignalActor
from ray.serve._private.constants import RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING
from ray.serve.handle import RayServeHandle


@ray.remote
class StreamingRequester:
    async def make_request(self) -> AsyncGenerator[str, None]:
        r = requests.get("http://localhost:8000", stream=True)
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=None, decode_unicode=True):
            yield chunk
            await asyncio.sleep(0.001)


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    reason="Streaming feature flag is disabled.",
)
@pytest.mark.parametrize("use_fastapi", [False, True])
@pytest.mark.parametrize("use_async", [False, True])
def test_basic(serve_instance, use_async: bool, use_fastapi: bool):
    async def hi_gen_async():
        for i in range(10):
            yield f"hi_{i}"

    def hi_gen_sync():
        for i in range(10):
            yield f"hi_{i}"

    if use_fastapi:
        app = FastAPI()

        @serve.deployment
        @serve.ingress(app)
        class SimpleGenerator:
            @app.get("/")
            def stream_hi(self, request: Request) -> StreamingResponse:
                gen = hi_gen_async() if use_async else hi_gen_sync()
                return StreamingResponse(gen, media_type="text/plain")

    else:

        @serve.deployment
        class SimpleGenerator:
            def __call__(self, request: Request) -> StreamingResponse:
                gen = hi_gen_async() if use_async else hi_gen_sync()
                return StreamingResponse(gen, media_type="text/plain")

    serve.run(SimpleGenerator.bind())

    r = requests.get("http://localhost:8000", stream=True)
    r.raise_for_status()
    for i, chunk in enumerate(r.iter_content(chunk_size=None, decode_unicode=True)):
        assert chunk == f"hi_{i}"


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    reason="Streaming feature flag is disabled.",
)
@pytest.mark.parametrize("use_fastapi", [False, True])
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("use_multiple_replicas", [False, True])
def test_responses_actually_streamed(
    serve_instance, use_fastapi: bool, use_async: bool, use_multiple_replicas: bool
):
    """Checks that responses are streamed as they are yielded.

    Also checks that responses can be streamed concurrently from a single replica
    or from multiple replicas.
    """
    signal_actor = SignalActor.remote()

    async def wait_on_signal_async():
        yield f"{os.getpid()}: before signal"
        await signal_actor.wait.remote()
        yield f"{os.getpid()}: after signal"

    def wait_on_signal_sync():
        yield f"{os.getpid()}: before signal"
        ray.get(signal_actor.wait.remote())
        yield f"{os.getpid()}: after signal"

    if use_fastapi:
        app = FastAPI()

        @serve.deployment
        @serve.ingress(app)
        class SimpleGenerator:
            @app.get("/")
            def stream(self, request: Request) -> StreamingResponse:
                gen = wait_on_signal_async() if use_async else wait_on_signal_sync()
                return StreamingResponse(gen, media_type="text/plain")

    else:

        @serve.deployment
        class SimpleGenerator:
            def __call__(self, request: Request) -> StreamingResponse:
                gen = wait_on_signal_async() if use_async else wait_on_signal_sync()
                return StreamingResponse(gen, media_type="text/plain")

    serve.run(
        SimpleGenerator.options(
            ray_actor_options={"num_cpus": 0},
            num_replicas=2 if use_multiple_replicas else 1,
        ).bind()
    )

    requester = StreamingRequester.remote()
    gen1 = requester.make_request.options(num_returns="streaming").remote()
    gen2 = requester.make_request.options(num_returns="streaming").remote()

    # Check that we get the first responses before the signal is sent
    # (so the generator is still hanging after the first yield).
    gen1_result = ray.get(next(gen1))
    gen2_result = ray.get(next(gen2))
    assert gen1_result.endswith("before signal")
    assert gen2_result.endswith("before signal")
    gen1_pid = gen1_result.split(":")[0]
    gen2_pid = gen2_result.split(":")[0]
    if use_multiple_replicas:
        assert gen1_pid != gen2_pid
    else:
        assert gen1_pid == gen2_pid

    # Check that the next obj_ref is not ready yet for both generators.
    assert gen1._next_sync(timeout_s=0.01).is_nil()
    assert gen2._next_sync(timeout_s=0.01).is_nil()

    # Now send signal to actor, second yield happens and we should get responses.
    ray.get(signal_actor.send.remote())
    gen1_result = ray.get(next(gen1))
    gen2_result = ray.get(next(gen2))
    assert gen1_result.startswith(gen1_pid)
    assert gen2_result.startswith(gen2_pid)
    assert gen1_result.endswith("after signal")
    assert gen2_result.endswith("after signal")

    # Client should be done getting messages.
    with pytest.raises(StopIteration):
        next(gen1)

    with pytest.raises(StopIteration):
        next(gen2)


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    reason="Streaming feature flag is disabled.",
)
@pytest.mark.parametrize("use_fastapi", [False, True])
def test_metadata_preserved(serve_instance, use_fastapi: bool):
    """Check that status code, headers, and media type are preserved."""

    def hi_gen():
        for i in range(10):
            yield f"hi_{i}"

    if use_fastapi:
        app = FastAPI()

        @serve.deployment
        @serve.ingress(app)
        class SimpleGenerator:
            @app.get("/")
            def stream_hi(self, request: Request) -> StreamingResponse:
                return StreamingResponse(
                    hi_gen(),
                    status_code=301,
                    headers={"hello": "world"},
                    media_type="foo/bar",
                )

    else:

        @serve.deployment
        class SimpleGenerator:
            def __call__(self, request: Request) -> StreamingResponse:
                return StreamingResponse(
                    hi_gen(),
                    status_code=301,
                    headers={"hello": "world"},
                    media_type="foo/bar",
                )

    serve.run(SimpleGenerator.bind())

    r = requests.get("http://localhost:8000", stream=True)
    assert r.status_code == 301
    assert r.headers["hello"] == "world"
    assert r.headers["content-type"] == "foo/bar"
    for i, chunk in enumerate(r.iter_content(chunk_size=None)):
        assert chunk == f"hi_{i}".encode("utf-8")


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    reason="Streaming feature flag is disabled.",
)
@pytest.mark.parametrize("use_fastapi", [False, True])
@pytest.mark.parametrize("use_async", [False, True])
def test_exception_in_generator(serve_instance, use_async: bool, use_fastapi: bool):
    async def hi_gen_async():
        yield "first result"
        raise Exception("raised in generator")

    def hi_gen_sync():
        yield "first result"
        raise Exception("raised in generator")

    if use_fastapi:
        app = FastAPI()

        @serve.deployment
        @serve.ingress(app)
        class SimpleGenerator:
            @app.get("/")
            def stream_hi(self, request: Request) -> StreamingResponse:
                gen = hi_gen_async() if use_async else hi_gen_sync()
                return StreamingResponse(gen, media_type="text/plain")

    else:

        @serve.deployment
        class SimpleGenerator:
            def __call__(self, request: Request) -> StreamingResponse:
                gen = hi_gen_async() if use_async else hi_gen_sync()
                return StreamingResponse(gen, media_type="text/plain")

    serve.run(SimpleGenerator.bind())

    r = requests.get("http://localhost:8000", stream=True)
    r.raise_for_status()
    stream_iter = r.iter_content(chunk_size=None, decode_unicode=True)
    assert next(stream_iter) == "first result"
    with pytest.raises(requests.exceptions.ChunkedEncodingError):
        next(stream_iter)


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    reason="Streaming feature flag is disabled.",
)
@pytest.mark.parametrize("use_fastapi", [False, True])
@pytest.mark.parametrize("use_async", [False, True])
def test_proxy_from_streaming_handle(
    serve_instance, use_async: bool, use_fastapi: bool
):
    @serve.deployment
    class Streamer:
        async def hi_gen_async(self):
            for i in range(10):
                yield f"hi_{i}"

        def hi_gen_sync(self):
            for i in range(10):
                yield f"hi_{i}"

    if use_fastapi:
        app = FastAPI()

        @serve.deployment
        @serve.ingress(app)
        class SimpleGenerator:
            def __init__(self, handle: RayServeHandle):
                self._h = handle.options(stream=True)

            @app.get("/")
            def stream_hi(self, request: Request) -> StreamingResponse:
                async def consume_obj_ref_gen():
                    if use_async:
                        obj_ref_gen = await self._h.hi_gen_async.remote()
                    else:
                        obj_ref_gen = await self._h.hi_gen_sync.remote()
                    async for obj_ref in obj_ref_gen:
                        yield await obj_ref

                return StreamingResponse(consume_obj_ref_gen(), media_type="text/plain")

    else:

        @serve.deployment
        class SimpleGenerator:
            def __init__(self, handle: RayServeHandle):
                self._h = handle.options(stream=True)

            def __call__(self, request: Request) -> StreamingResponse:
                async def consume_obj_ref_gen():
                    if use_async:
                        obj_ref_gen = await self._h.hi_gen_async.remote()
                    else:
                        obj_ref_gen = await self._h.hi_gen_sync.remote()
                    async for obj_ref in obj_ref_gen:
                        yield await obj_ref

                return StreamingResponse(consume_obj_ref_gen(), media_type="text/plain")

    serve.run(SimpleGenerator.bind(Streamer.bind()))

    r = requests.get("http://localhost:8000", stream=True)
    r.raise_for_status()
    for i, chunk in enumerate(r.iter_content(chunk_size=None, decode_unicode=True)):
        assert chunk == f"hi_{i}"


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    reason="Streaming feature flag is disabled.",
)
def test_http_disconnect(serve_instance):
    """Test that response generators are cancelled when the client disconnects."""
    signal_actor = SignalActor.remote()

    @serve.deployment
    class SimpleGenerator:
        def __call__(self, request: Request) -> StreamingResponse:
            async def wait_for_disconnect():
                try:
                    yield "hi"
                    await asyncio.sleep(100)
                except asyncio.CancelledError:
                    print("Cancelled!")
                    signal_actor.send.remote()

            return StreamingResponse(wait_for_disconnect())

    serve.run(SimpleGenerator.bind())

    with requests.get("http://localhost:8000", stream=True):
        with pytest.raises(TimeoutError):
            ray.get(signal_actor.wait.remote(), timeout=1)

    ray.get(signal_actor.wait.remote(), timeout=5)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
