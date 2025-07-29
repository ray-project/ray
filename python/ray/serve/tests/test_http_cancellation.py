import asyncio
import sys

import httpx
import pytest
from fastapi import FastAPI
from starlette.requests import Request

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.test_utils import (
    get_application_url,
    send_signal_on_cancellation,
)
from ray.serve.exceptions import RequestCancelledError


@ray.remote
class Collector:
    def __init__(self):
        self.items = []

    def add(self, item):
        self.items.append(item)

    def get(self):
        return self.items


def test_collector_class(serve_instance):
    collector = Collector.remote()

    random_items = ["this", "is", 1, "demo", "string"]

    for item in random_items:
        collector.add.remote(item)

    result = ray.get(collector.get.remote())

    assert len(result) == len(random_items)

    for i in range(0, len(result)):
        assert result[i] == random_items[i]


@pytest.mark.parametrize("use_fastapi", [False, True])
def test_cancel_on_http_client_disconnect_during_execution(
    serve_instance, use_fastapi: bool
):
    """Test the client disconnecting while the handler is executing."""
    inner_signal_actor = SignalActor.remote()
    outer_signal_actor = SignalActor.remote()

    @serve.deployment
    async def inner():
        async with send_signal_on_cancellation(inner_signal_actor):
            pass

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
                async with send_signal_on_cancellation(outer_signal_actor):
                    pass

    else:

        @serve.deployment
        class Ingress:
            def __init__(self, handle):
                self._handle = handle

            async def __call__(self, request: Request):
                _ = self._handle.remote()
                async with send_signal_on_cancellation(outer_signal_actor):
                    pass

    serve.run(Ingress.bind(inner.bind()))

    # Intentionally time out on the client, causing it to disconnect.
    with pytest.raises(httpx.ReadTimeout):
        httpx.get(get_application_url("HTTP"), timeout=0.5)

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
    with pytest.raises(httpx.ReadTimeout):
        httpx.get(get_application_url("HTTP"), timeout=0.5)

    # Now signal the initial request to finish and check that the request sent via HTTP
    # never reaches the replica.
    ray.get(signal_actor.send.remote())
    assert initial_response.result() == 1
    for i in range(2, 12):
        assert h.remote().result() == i


@pytest.mark.asyncio
async def test_request_cancelled_error_on_http_client_disconnect_during_execution(
    serve_instance,
):
    """Test the exception thrown for executing request on http client disconnect"""
    collector = Collector.remote()
    child_signal = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=1)
    class Child:
        async def __call__(self):
            try:
                await child_signal.wait.remote()
            except asyncio.CancelledError:
                await collector.add.remote("Child_CancelledError")
                raise

    @serve.deployment
    class Parent:
        def __init__(self, child):
            self.child = child

        async def __call__(self):
            try:
                await self.child.remote()
            except asyncio.CancelledError:
                await collector.add.remote("Parent_AsyncioCancelledError")
                raise
            except RequestCancelledError:
                await collector.add.remote("Parent_RequestCancelledError")
                raise

    serve.run(Parent.bind(Child.bind()))

    # Make a request with short timeout that will cause disconnection
    try:
        await httpx.AsyncClient(timeout=0.5).get(get_application_url("HTTP"))
    except httpx.ReadTimeout:
        pass

    wait_for_condition(
        lambda: set(ray.get(collector.get.remote()))
        == {"Child_CancelledError", "Parent_AsyncioCancelledError"}
    )


@pytest.mark.asyncio
async def test_request_cancelled_error_on_http_client_disconnect_during_assignment(
    serve_instance,
):
    """Test the exception thrown for queued request on http client disconnect"""
    collector = Collector.remote()
    child_signal = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=1)
    class Child:
        async def __call__(self):
            try:
                await child_signal.wait.remote()
            except asyncio.CancelledError:
                await collector.add.remote("Child_CancelledError")
                raise

    @serve.deployment
    class Parent:
        def __init__(self, child):
            self.child = child

        async def __call__(self):
            try:
                await self.child.remote()
            except asyncio.CancelledError:
                await collector.add.remote("Parent_AsyncioCancelledError")
                raise
            except RequestCancelledError:
                await collector.add.remote("Parent_RequestCancelledError")
                raise

    h = serve.run(Parent.bind(Child.bind()))

    # Block Child with first request
    r = h.remote()
    wait_for_condition(lambda: ray.get(child_signal.cur_num_waiters.remote()) == 1)

    # Make a second request with short timeout that will cause disconnection
    try:
        await httpx.AsyncClient(timeout=0.5).get(get_application_url("HTTP"))
    except httpx.ReadTimeout:
        pass

    wait_for_condition(
        lambda: ray.get(collector.get.remote()) == ["Parent_AsyncioCancelledError"]
    )

    # Clean up first request
    r.cancel()
    try:
        await r
    except RequestCancelledError:
        pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
