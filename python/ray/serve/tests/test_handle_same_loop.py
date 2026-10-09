import asyncio
import sys

import httpx
import pytest

from ray import serve
from ray._common.test_utils import SignalActor, async_wait_for_condition
from ray.serve._private.constants import (
    RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP,
)
from ray.serve._private.test_utils import get_application_url
from ray.serve.exceptions import RequestCancelledError
from ray.serve.handle import (
    DeploymentHandle,
)


@pytest.fixture
def _skip_test_if_router_running_in_separate_loop():
    if RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP:
        pytest.skip("Router is running in a separate loop.")


@pytest.mark.asyncio
async def test_deployment_handle_works_with_await_when_router_in_same_loop(
    serve_instance_async, _skip_test_if_router_running_in_separate_loop
):
    @serve.deployment
    class F:
        async def __call__(self):
            return "hi"

    h = serve.run(F.bind())
    assert await h.remote() == "hi"


def test_deployment_handle_result_fails_when_driver_not_in_async_loop(
    serve_instance, _skip_test_if_router_running_in_separate_loop
):
    @serve.deployment
    class F:
        def __call__(self):
            return "hi"

    h = serve.run(F.bind())
    with pytest.raises(RuntimeError):
        h.remote().result()


@pytest.mark.asyncio
async def test_deployment_handle_result_fails_in_async_context_but_await_succeeds(
    serve_instance_async, _skip_test_if_router_running_in_separate_loop
):
    @serve.deployment
    class F:
        def __call__(self):
            return "hi"

    h = serve.run(F.bind())
    with pytest.raises(RuntimeError):
        h.remote().result()

    assert await h.remote() == "hi"


def test_http_proxy_requests_work_when_router_in_same_loop(
    serve_instance, _skip_test_if_router_running_in_separate_loop
):
    @serve.deployment
    class F:
        def __call__(self):
            return "hi"

    serve.run(F.bind())
    url = "http://localhost:8000/"

    resp = httpx.get(url)
    assert resp.status_code == 200
    assert resp.text == "hi"


@pytest.mark.asyncio
async def test_deployment_handle_configured_for_same_loop_via_init(
    serve_instance_async,
):
    @serve.deployment
    class F:
        def __call__(self):
            return "hi"

    h = serve.run(F.bind())
    h._init(_run_router_in_separate_loop=False)
    assert await h.remote() == "hi"

    with pytest.raises(RuntimeError):
        h.remote().result()


def test_child_deployment_handle_configured_for_same_loop_communication(serve_instance):
    @serve.deployment
    class Child:
        def __call__(self):
            return "hi"

    @serve.deployment
    class Parent:
        def __init__(self, child_handle: DeploymentHandle):
            self.child_handle = child_handle
            self.child_handle._init(_run_router_in_separate_loop=False)

        async def __call__(self):
            return await self.child_handle.remote()

    serve.run(Parent.bind(Child.bind()))
    url = get_application_url("HTTP")
    resp = httpx.get(url)
    assert resp.status_code == 200
    assert resp.text == "hi"


@pytest.mark.asyncio
async def test_deployment_handle_exception_propagation_in_same_loop(
    serve_instance_async, _skip_test_if_router_running_in_separate_loop
):
    """Test that exceptions are properly propagated when router runs in same loop."""

    @serve.deployment
    class FailingDeployment:
        def __call__(self):
            raise ValueError("Intentional test error")

    h = serve.run(FailingDeployment.bind())

    with pytest.raises(ValueError, match="Intentional test error"):
        await h.remote()


@pytest.mark.asyncio
async def test_streaming_response_generator_in_same_loop(
    serve_instance_async, _skip_test_if_router_running_in_separate_loop
):
    """Test that streaming responses work correctly when router runs in same loop."""

    @serve.deployment
    class StreamingDeployment:
        def generate_numbers(self, limit: int):
            for i in range(limit):
                yield i

    h = serve.run(StreamingDeployment.bind())
    streaming_handle = h.options(stream=True)

    gen = streaming_handle.generate_numbers.remote(5)
    results = []
    async for value in gen:
        results.append(value)

    assert results == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_concurrent_requests_in_same_loop(
    serve_instance_async, _skip_test_if_router_running_in_separate_loop
):
    """Test that multiple concurrent requests work correctly in same loop mode."""

    @serve.deployment
    class ConcurrentDeployment:
        async def slow_operation(self, delay: float, value: str):
            await asyncio.sleep(delay)
            return f"result-{value}"

    h = serve.run(ConcurrentDeployment.bind())

    # Launch multiple concurrent requests
    tasks = [
        h.slow_operation.remote(0.1, "a"),
        h.slow_operation.remote(0.1, "b"),
        h.slow_operation.remote(0.1, "c"),
    ]

    # All should complete successfully
    results = await asyncio.gather(*tasks)
    assert set(results) == {"result-a", "result-b", "result-c"}


@pytest.mark.asyncio
async def test_request_cancellation_in_same_loop(
    serve_instance_async, _skip_test_if_router_running_in_separate_loop
):
    """Test that request cancellation works correctly when router runs in same loop."""
    signal_actor = SignalActor.remote()

    @serve.deployment
    class SlowDeployment:
        async def slow_operation(self):
            await signal_actor.wait.remote()
            return "should_not_reach_here"

    h = serve.run(SlowDeployment.bind())

    response = h.slow_operation.remote()

    async def check_num_waiters():
        assert await signal_actor.cur_num_waiters.remote() == 1
        return True

    # its important that we use async_wait_for_condition here because
    # if we block the event loop then router wont be able to function
    async_wait_for_condition(check_num_waiters, timeout=10)

    # Cancel the request
    response.cancel()

    # Should raise CancelledError
    with pytest.raises(RequestCancelledError):
        await response

    await signal_actor.send.remote(clear=True)


@pytest.mark.asyncio
async def test_multiple_awaits(serve_instance_async):
    """Test that multiple awaits doesn't call replica multiple times."""
    a = 0

    @serve.deployment
    async def foo():
        nonlocal a
        a += 1
        return a

    app = serve.run(foo.bind())

    response = app.remote()
    assert await response == 1
    assert await response == 1

    response = app.remote()
    assert await response == 2
    assert await response == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
