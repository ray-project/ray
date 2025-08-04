import asyncio
import sys

import httpx
import pytest
from anyio import to_thread
from fastapi import FastAPI
from starlette.responses import PlainTextResponse

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.constants import RAY_SERVE_RUN_SYNC_IN_THREADPOOL
from ray.serve._private.test_utils import get_application_url


@pytest.mark.skipif(
    not RAY_SERVE_RUN_SYNC_IN_THREADPOOL,
    reason="Run sync method in threadpool FF disabled.",
)
@pytest.mark.parametrize("use_fastapi", [False, True])
def test_not_running_in_asyncio_loop(serve_instance, use_fastapi: bool):
    if use_fastapi:
        fastapi_app = FastAPI()

        @serve.deployment
        @serve.ingress(fastapi_app)
        class D:
            @fastapi_app.get("/")
            def root(self):
                with pytest.raises(RuntimeError, match="no running event loop"):
                    asyncio.get_running_loop()

    else:

        @serve.deployment
        class D:
            def __call__(self) -> str:
                with pytest.raises(RuntimeError, match="no running event loop"):
                    asyncio.get_running_loop()

    serve.run(D.bind())
    # Would error if the check fails.
    base_url = get_application_url()
    httpx.get(f"{base_url}/").raise_for_status()


@pytest.mark.skipif(
    not RAY_SERVE_RUN_SYNC_IN_THREADPOOL,
    reason="Run sync method in threadpool FF disabled.",
)
def test_concurrent_execution(serve_instance):
    signal_actor = SignalActor.remote()

    @serve.deployment
    class D:
        def do_sync(self):
            ray.get(signal_actor.wait.remote())

        async def do_async(self):
            await signal_actor.wait.remote()

    h = serve.run(D.bind())

    sync_results = [h.do_sync.remote(), h.do_sync.remote()]
    async_results = [h.do_async.remote(), h.do_async.remote()]

    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 4)
    ray.get(signal_actor.send.remote())
    [r.result() for r in sync_results + async_results]


@pytest.mark.skipif(
    not RAY_SERVE_RUN_SYNC_IN_THREADPOOL,
    reason="Run sync method in threadpool FF disabled.",
)
@pytest.mark.parametrize("use_fastapi", [False, True])
def test_context_vars_propagated(serve_instance, use_fastapi: bool):
    if use_fastapi:
        fastapi_app = FastAPI()

        @serve.deployment
        @serve.ingress(fastapi_app)
        class D:
            @fastapi_app.get("/")
            def root(self):
                return PlainTextResponse(
                    serve.context._get_serve_request_context().request_id
                )

    else:

        @serve.deployment
        class D:
            def __call__(self) -> str:
                return PlainTextResponse(
                    serve.context._get_serve_request_context().request_id
                )

    serve.run(D.bind())

    base_url = get_application_url()
    r = httpx.get(f"{base_url}/", headers={"X-Request-Id": "TEST-ID"})
    r.raise_for_status()
    # If context vars weren't propagated, the request ID would be empty.
    assert r.text == "TEST-ID"


@pytest.mark.skipif(
    not RAY_SERVE_RUN_SYNC_IN_THREADPOOL,
    reason="Run sync method in threadpool FF disabled.",
)
def test_thread_limit_set_to_max_ongoing_requests(serve_instance):
    @serve.deployment
    class D:
        async def __call__(self):
            return to_thread.current_default_thread_limiter().total_tokens

    h = serve.run(D.bind())

    # Check that it's set if max_ongoing_requests is defaulted.
    assert h.remote().result() == 5

    # Update to a custom value, check again.
    h = serve.run(D.options(max_ongoing_requests=10).bind())
    assert h.remote().result() == 10


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
