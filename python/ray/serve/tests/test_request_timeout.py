import asyncio
import os
import sys
import time
from typing import Generator, Set

import pytest
import requests
from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import StreamingResponse

import ray
from ray import serve
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient
from ray.serve._private.common import ApplicationStatus
from ray.serve.schema import ServeInstanceDetails
from ray.serve.tests.common.utils import send_signal_on_cancellation
from ray.util.state import list_tasks


@ray.remote
def do_request():
    return requests.get("http://localhost:8000")


@pytest.fixture
def shutdown_serve():
    yield
    serve.shutdown()


@pytest.mark.parametrize(
    "ray_instance",
    [
        {"RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "5"},
    ],
    indirect=True,
)
def test_normal_operation(ray_instance, shutdown_serve):
    """
    Verify that a moderate timeout doesn't affect normal operation.
    """

    @serve.deployment(num_replicas=2)
    def f(*args):
        return "Success!"

    serve.run(f.bind())

    assert all(
        response.text == "Success!"
        for response in ray.get([do_request.remote() for _ in range(10)])
    )


@pytest.mark.parametrize(
    "ray_instance",
    [
        {"RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0.1"},
    ],
    indirect=True,
)
def test_request_hangs_in_execution(ray_instance, shutdown_serve):
    """
    Verify that requests are timed out if they take longer than the timeout to execute.
    """

    @ray.remote
    class PidTracker:
        def __init__(self):
            self.pids = set()

        def add_pid(self, pid: int) -> None:
            self.pids.add(pid)

        def get_pids(self) -> Set[int]:
            return self.pids

    pid_tracker = PidTracker.remote()
    signal_actor = SignalActor.remote()

    @serve.deployment(num_replicas=2, graceful_shutdown_timeout_s=0)
    class HangsOnFirstRequest:
        def __init__(self):
            self._saw_first_request = False

        async def __call__(self):
            ray.get(pid_tracker.add_pid.remote(os.getpid()))
            if not self._saw_first_request:
                self._saw_first_request = True
                await asyncio.sleep(10)

            return "Success!"

    serve.run(HangsOnFirstRequest.bind())

    response = requests.get("http://localhost:8000")
    assert response.status_code == 408

    ray.get(signal_actor.send.remote())


@serve.deployment(graceful_shutdown_timeout_s=0)
class HangsOnFirstRequest:
    def __init__(self):
        self._saw_first_request = False
        self.signal_actor = SignalActor.remote()

    async def __call__(self):
        if not self._saw_first_request:
            self._saw_first_request = True
            await self.signal_actor.wait.remote()
        else:
            ray.get(self.signal_actor.send.remote())
        return "Success!"


hangs_on_first_request_app = HangsOnFirstRequest.bind()


def test_with_rest_api(ray_start_stop):
    """Verify the REST API can configure the request timeout."""
    config = {
        "proxy_location": "EveryNode",
        "http_options": {"request_timeout_s": 1},
        "applications": [
            {
                "name": "app",
                "route_prefix": "/",
                "import_path": (
                    "ray.serve.tests." "test_request_timeout:hangs_on_first_request_app"
                ),
            }
        ],
    }
    ServeSubmissionClient("http://localhost:8265").deploy_applications(config)

    def application_running():
        response = requests.get(
            "http://localhost:52365/api/serve/applications/", timeout=15
        )
        assert response.status_code == 200

        serve_details = ServeInstanceDetails(**response.json())
        return serve_details.applications["app"].status == ApplicationStatus.RUNNING

    wait_for_condition(application_running, timeout=15)
    print("Application has started running. Testing requests...")

    response = requests.get("http://localhost:8000")
    assert response.status_code == 408

    response = requests.get("http://localhost:8000")
    assert response.status_code == 200
    print("Requests succeeded! Deleting application.")
    ServeSubmissionClient("http://localhost:8265").delete_applications()


@pytest.mark.parametrize(
    "ray_instance",
    [
        {"RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0.5"},
    ],
    indirect=True,
)
def test_request_hangs_in_assignment(ray_instance, shutdown_serve):
    """
    Verify that requests are timed out if they take longer than the timeout while
    pending assignment (queued in the handle).
    """
    signal_actor = SignalActor.remote()

    @serve.deployment(graceful_shutdown_timeout_s=0, max_concurrent_queries=1)
    class HangsOnFirstRequest:
        def __init__(self):
            self._saw_first_request = False

        async def __call__(self):
            await signal_actor.wait.remote()
            return "Success!"

    serve.run(HangsOnFirstRequest.bind())

    # First request will hang executing, second pending assignment.
    response_ref1 = do_request.remote()
    response_ref2 = do_request.remote()

    # Streaming path does not retry on timeouts, so the requests should be failed.
    assert ray.get(response_ref1).status_code == 408
    assert ray.get(response_ref2).status_code == 408
    ray.get(signal_actor.send.remote())
    assert ray.get(do_request.remote()).status_code == 200


@pytest.mark.parametrize(
    "ray_instance",
    [
        {"RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0.1"},
    ],
    indirect=True,
)
def test_streaming_request_already_sent_and_timed_out(ray_instance, shutdown_serve):
    """
    Verify that streaming requests are timed out even if some chunks have already
    been sent.
    """

    @serve.deployment(graceful_shutdown_timeout_s=0, max_concurrent_queries=1)
    class SleepForNSeconds:
        def __init__(self, sleep_s: int):
            self.sleep_s = sleep_s

        def generate_numbers(self) -> Generator[str, None, None]:
            for i in range(2):
                yield f"generated {i}"
                time.sleep(self.sleep_s)

        def __call__(self, request: Request) -> StreamingResponse:
            gen = self.generate_numbers()
            return StreamingResponse(gen, status_code=200, media_type="text/plain")

    serve.run(SleepForNSeconds.bind(0.11))  # 0.11s > 0.1s timeout

    r = requests.get("http://localhost:8000", stream=True)
    iterator = r.iter_content(chunk_size=None, decode_unicode=True)

    # The first chunk should be received successfully.
    assert iterator.__next__() == "generated 0"
    assert r.status_code == 200

    # The second chunk should time out and raise error.
    with pytest.raises(requests.exceptions.ChunkedEncodingError) as request_error:
        iterator.__next__()
    assert "Connection broken" in str(request_error.value)


@pytest.mark.parametrize(
    "ray_instance",
    [
        {"RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0.5"},
    ],
    indirect=True,
)
def test_request_timeout_does_not_leak_tasks(ray_instance, shutdown_serve):
    """Verify that the ASGI-related tasks exit when a request is timed out.

    See https://github.com/ray-project/ray/issues/38368 for details.
    """

    @serve.deployment
    class Hang:
        async def __call__(self):
            await asyncio.sleep(1000000)

    serve.run(Hang.bind())

    def get_num_running_tasks():
        return len(
            list_tasks(
                address=ray_instance["gcs_address"],
                filters=[
                    ("NAME", "!=", "ServeController.listen_for_change"),
                    ("TYPE", "=", "ACTOR_TASK"),
                    ("STATE", "=", "RUNNING"),
                ],
            )
        )

    wait_for_condition(lambda: get_num_running_tasks() == 0)

    # Send a number of requests that all will be timed out.
    results = ray.get([do_request.remote() for _ in range(10)])
    assert all(r.status_code == 408 for r in results)

    # The tasks should all be cancelled.
    wait_for_condition(lambda: get_num_running_tasks() == 0)


@pytest.mark.parametrize(
    "ray_instance",
    [
        {
            "RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0.5",
        },
    ],
    indirect=True,
)
@pytest.mark.parametrize("use_fastapi", [False, True])
def test_cancel_on_http_timeout_during_execution(
    ray_instance, shutdown_serve, use_fastapi: bool
):
    """Test the request timing out while the handler is executing."""
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
                self._handle = handle.options(use_new_handle_api=True)

            @app.get("/")
            async def wait_for_cancellation(self):
                await self._handle.remote()._to_object_ref()
                await send_signal_on_cancellation(outer_signal_actor)

    else:

        @serve.deployment
        class Ingress:
            def __init__(self, handle):
                self._handle = handle.options(use_new_handle_api=True)

            async def __call__(self, request: Request):
                await self._handle.remote()._to_object_ref()
                await send_signal_on_cancellation(outer_signal_actor)

    serve.run(Ingress.bind(inner.bind()))

    # Request should time out, causing the handler and handle call to be cancelled.
    assert requests.get("http://localhost:8000").status_code == 408
    ray.get(inner_signal_actor.wait.remote())
    ray.get(outer_signal_actor.wait.remote())


@pytest.mark.parametrize(
    "ray_instance",
    [
        {
            "RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0.5",
        },
    ],
    indirect=True,
)
def test_cancel_on_http_timeout_during_assignment(ray_instance, shutdown_serve):
    """Test the client disconnecting while the proxy is assigning the request."""
    signal_actor = SignalActor.remote()

    @serve.deployment(max_concurrent_queries=1)
    class Ingress:
        def __init__(self):
            self._num_requests = 0

        async def __call__(self, *args):
            self._num_requests += 1
            await signal_actor.wait.remote()

            return self._num_requests

    h = serve.run(Ingress.bind()).options(use_new_handle_api=True)

    # Send a request and wait for it to be ongoing so we know that further requests
    # will be blocking trying to assign a replica.
    initial_response = h.remote()
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 1)

    # Request should time out, causing the handler and handle call to be cancelled.
    assert requests.get("http://localhost:8000").status_code == 408

    # Now signal the initial request to finish and check that the request sent via HTTP
    # never reaches the replica.
    ray.get(signal_actor.send.remote())
    assert initial_response.result() == 1
    for i in range(2, 12):
        assert h.remote().result() == i


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
