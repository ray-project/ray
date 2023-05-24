import pytest
from typing import Generator

from fastapi import FastAPI
import requests
from starlette.responses import StreamingResponse
from starlette.requests import Request

import ray
from ray._private.test_utils import SignalActor

from ray import serve
from ray.serve._private.constants import RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING


@ray.remote
class StreamingRequester:
    def make_request(self) -> Generator[str, None, None]:
        r = requests.get("http://localhost:8000", stream=True)
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=None, decode_unicode=True):
            yield chunk


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
def test_response_actually_streamed(serve_instance, use_fastapi: bool, use_async: bool):
    """Checks that responses are streamed as they are yielded."""
    signal_actor = SignalActor.remote()

    async def wait_on_signal_async():
        yield "before signal"
        await signal_actor.wait.remote()
        yield "after signal"

    async def wait_on_signal_sync():
        yield "before signal"
        ray.get(signal_actor.wait.remote())
        yield "after signal"

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

    serve.run(SimpleGenerator.bind())

    requester = StreamingRequester.remote()
    gen = requester.make_request.options(num_returns="streaming").remote()

    # Check that we get the first response before the signal is sent
    # (so the generator is still hanging after the first yield).
    obj_ref = next(gen)
    assert ray.get(obj_ref) == "before signal"

    # Check that the next obj_ref is not ready yet.
    obj_ref = gen._next_sync(timeout_s=0.01)
    assert obj_ref.is_nil()

    # Now send signal to actor, second yield happens.
    ray.get(signal_actor.send.remote())
    obj_ref = next(gen)
    assert ray.get(obj_ref) == "after signal"

    # Client should be done getting messages.
    with pytest.raises(StopIteration):
        next(gen)


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
