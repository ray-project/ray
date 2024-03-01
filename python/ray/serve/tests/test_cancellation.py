import asyncio
import concurrent.futures
import sys

import pytest
import requests
from fastapi import FastAPI
from starlette.requests import Request

import ray
from ray import serve
from ray._private.test_utils import (
    SignalActor,
    async_wait_for_condition,
    wait_for_condition,
)
from ray.serve._private.test_utils import send_signal_on_cancellation


@pytest.mark.parametrize("use_fastapi", [False, True])
def test_cancel_on_http_client_disconnect_during_execution(
    serve_instance, use_fastapi: bool
):
    """Test the client disconnecting while the handler is executing."""
    inner_signal_actor = SignalActor.remote()
    outer_signal_actor = SignalActor.remote()

    @serve.deployment
    async def inner():
        await send_signal_on_cancellation(inner_signal_actor)

    if use_fastapi:
        app = FastAPI()

        @serve.deployment
        @serve.ingress(app)
        class Ingress:
            def __init__(self, handle):
                self._handle = handle

            @app.get("/")
            async def wait_for_cancellation(self):
                _ = self._handle.remote()
                await send_signal_on_cancellation(outer_signal_actor)

    else:

        @serve.deployment
        class Ingress:
            def __init__(self, handle):
                self._handle = handle

            async def __call__(self, request: Request):
                _ = self._handle.remote()
                await send_signal_on_cancellation(outer_signal_actor)

    serve.run(Ingress.bind(inner.bind()))

    # Intentionally time out on the client, causing it to disconnect.
    with pytest.raises(requests.exceptions.ReadTimeout):
        requests.get("http://localhost:8000", timeout=0.5)

    # Both the HTTP handler and the inner deployment handle call should be cancelled.
    ray.get(inner_signal_actor.wait.remote(), timeout=10)
    ray.get(outer_signal_actor.wait.remote(), timeout=10)


def test_cancel_on_http_client_disconnect_during_assignment(serve_instance):
    """Test the client disconnecting while the proxy is assigning the request."""
    signal_actor = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=1)
    class Ingress:
        def __init__(self):
            self._num_requests = 0

        async def __call__(self, *args):
            self._num_requests += 1
            await signal_actor.wait.remote()

            return self._num_requests

    h = serve.run(Ingress.bind())

    # Send a request and wait for it to be ongoing so we know that further requests
    # will block trying to assign a replica.
    initial_response = h.remote()
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 1)

    # Intentionally time out on the client, causing it to disconnect.
    with pytest.raises(requests.exceptions.ReadTimeout):
        requests.get("http://localhost:8000", timeout=0.5)

    # Now signal the initial request to finish and check that the request sent via HTTP
    # never reaches the replica.
    ray.get(signal_actor.send.remote())
    assert initial_response.result() == 1
    for i in range(2, 12):
        assert h.remote().result() == i


def test_cancel_sync_handle_call_during_execution(serve_instance):
    """Test cancelling handle request during execution (sync context)."""
    running_signal_actor = SignalActor.remote()
    cancelled_signal_actor = SignalActor.remote()

    @serve.deployment
    class Ingress:
        async def __call__(self, *args):
            await running_signal_actor.send.remote()
            await send_signal_on_cancellation(cancelled_signal_actor)

    h = serve.run(Ingress.bind())

    # Send a request and wait for it to start executing.
    r = h.remote()
    ray.get(running_signal_actor.wait.remote(), timeout=10)

    # Cancel it and verify that it is cancelled via signal.
    r.cancel()
    ray.get(cancelled_signal_actor.wait.remote(), timeout=10)

    with pytest.raises(ray.exceptions.TaskCancelledError):
        r.result()


def test_cancel_sync_handle_call_during_assignment(serve_instance):
    """Test cancelling handle request during assignment (sync context)."""
    signal_actor = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=1)
    class Ingress:
        def __init__(self):
            self._num_requests = 0

        async def __call__(self, *args):
            self._num_requests += 1
            await signal_actor.wait.remote()

            return self._num_requests

    h = serve.run(Ingress.bind())

    # Send a request and wait for it to be ongoing so we know that further requests
    # will block trying to assign a replica.
    initial_response = h.remote()
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 1)

    # Make a second request, cancel it, and verify that it is cancelled.
    second_response = h.remote()
    second_response.cancel()
    with pytest.raises(concurrent.futures.CancelledError):
        second_response.result()

    # Now signal the initial request to finish and check that the second request
    # never reached the replica.
    ray.get(signal_actor.send.remote())
    assert initial_response.result() == 1
    for i in range(2, 12):
        assert h.remote().result() == i


def test_cancel_async_handle_call_during_execution(serve_instance):
    """Test cancelling handle request during execution (async context)."""
    running_signal_actor = SignalActor.remote()
    cancelled_signal_actor = SignalActor.remote()

    @serve.deployment
    class Downstream:
        async def __call__(self, *args):
            await running_signal_actor.send.remote()
            await send_signal_on_cancellation(cancelled_signal_actor)

    @serve.deployment
    class Ingress:
        def __init__(self, handle):
            self._h = handle

        async def __call__(self, *args):
            # Send a request and wait for it to start executing.
            r = self._h.remote()
            await running_signal_actor.wait.remote()

            # Cancel it and verify that it is cancelled via signal.
            r.cancel()
            await cancelled_signal_actor.wait.remote()

            with pytest.raises(ray.exceptions.TaskCancelledError):
                await r

    h = serve.run(Ingress.bind(Downstream.bind()))
    h.remote().result()  # Would raise if test failed.


def test_cancel_async_handle_call_during_assignment(serve_instance):
    """Test cancelling handle request during assignment (async context)."""
    signal_actor = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=1)
    class Downstream:
        def __init__(self):
            self._num_requests = 0

        async def __call__(self, *args):
            self._num_requests += 1
            await signal_actor.wait.remote()

            return self._num_requests

    @serve.deployment
    class Ingress:
        def __init__(self, handle):
            self._h = handle

        async def __call__(self, *args):
            # Send a request and wait for it to be ongoing so we know that further
            # requests will block trying to assign a replica.
            initial_response = self._h.remote()

            async def one_waiter():
                return await signal_actor.cur_num_waiters.remote() == 1

            await async_wait_for_condition(one_waiter)

            # Make a second request, cancel it, and verify that it is cancelled.
            second_response = self._h.remote()
            second_response.cancel()
            with pytest.raises(asyncio.CancelledError):
                await second_response

            # Now signal the initial request to finish and check that the second request
            # never reached the replica.
            await signal_actor.send.remote()
            assert await initial_response == 1
            for i in range(2, 12):
                assert await self._h.remote() == i

    h = serve.run(Ingress.bind(Downstream.bind()))
    h.remote().result()  # Would raise if test failed.


def test_cancel_generator_sync(serve_instance):
    """Test cancelling streaming handle request during execution."""
    signal_actor = SignalActor.remote()

    @serve.deployment
    class Ingress:
        async def __call__(self, *args):
            yield "hi"
            await send_signal_on_cancellation(signal_actor)

    h = serve.run(Ingress.bind()).options(stream=True)

    # Send a request and wait for it to start executing.
    g = h.remote()

    assert next(g) == "hi"

    # Cancel it and verify that it is cancelled via signal.
    g.cancel()

    with pytest.raises(ray.exceptions.TaskCancelledError):
        next(g)

    ray.get(signal_actor.wait.remote(), timeout=10)


def test_cancel_generator_async(serve_instance):
    """Test cancelling streaming handle request during execution."""
    signal_actor = SignalActor.remote()

    @serve.deployment
    class Downstream:
        async def __call__(self, *args):
            yield "hi"
            await send_signal_on_cancellation(signal_actor)

    @serve.deployment
    class Ingress:
        def __init__(self, handle):
            self._h = handle.options(stream=True)

        async def __call__(self, *args):
            # Send a request and wait for it to start executing.
            g = self._h.remote()
            assert await g.__anext__() == "hi"

            # Cancel it and verify that it is cancelled via signal.
            g.cancel()

            with pytest.raises(ray.exceptions.TaskCancelledError):
                assert await g.__anext__() == "hi"

            await signal_actor.wait.remote()

    h = serve.run(Ingress.bind(Downstream.bind()))
    h.remote().result()  # Would raise if test failed.


def test_only_relevant_task_is_cancelled(serve_instance):
    """Test cancelling one request doesn't affect others."""
    signal_actor = SignalActor.remote()

    @serve.deployment
    class Ingress:
        async def __call__(self, *args):
            await signal_actor.wait.remote()
            return "ok"

    h = serve.run(Ingress.bind())

    r1 = h.remote()
    r2 = h.remote()

    # Wait for both requests to be executing.
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 2)

    r1.cancel()
    with pytest.raises(ray.exceptions.TaskCancelledError):
        r1.result()

    # Now signal r2 to run to completion and check that it wasn't cancelled.
    ray.get(signal_actor.send.remote())
    assert r2.result() == "ok"


def test_out_of_band_task_is_not_cancelled(serve_instance):
    """
    Test cancelling a request doesn't cancel tasks submitted
    outside the request's context.
    """
    signal_actor = SignalActor.remote()

    @serve.deployment
    class Downstream:
        async def hi(self):
            await signal_actor.wait.remote()
            return "ok"

    @serve.deployment
    class Ingress:
        def __init__(self, handle):
            self._h = handle
            self._out_of_band_req = self._h.hi.remote()

        async def __call__(self, *args):
            await self._h.hi.remote()

        async def get_out_of_band_response(self):
            return await self._out_of_band_req

    h = serve.run(Ingress.bind(Downstream.bind()))

    # Send a request, wait for downstream request to start, and cancel it.
    r1 = h.remote()
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 2)

    r1.cancel()
    with pytest.raises(ray.exceptions.TaskCancelledError):
        r1.result()

    # Now signal out of band request to run to completion and check that it wasn't
    # cancelled.
    ray.get(signal_actor.send.remote())
    assert h.get_out_of_band_response.remote().result() == "ok"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
