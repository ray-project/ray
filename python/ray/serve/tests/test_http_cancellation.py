import sys

import pytest
import requests
from fastapi import FastAPI
from starlette.requests import Request

import ray
from ray import serve
from ray._private.test_utils import SignalActor, wait_for_condition
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
