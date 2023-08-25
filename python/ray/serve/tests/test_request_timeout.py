import asyncio
import os
import sys
from typing import Generator, Set
import time

import pytest
import requests

import ray
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient
from ray.util.state import list_tasks
from ray._private.test_utils import SignalActor, wait_for_condition

from ray import serve
from ray.serve.schema import ServeInstanceDetails
from ray.serve._private.common import ApplicationStatus
from ray.serve._private.constants import RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING
from starlette.responses import StreamingResponse
from starlette.requests import Request


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
    if RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING:
        assert response.status_code == 408
    else:
        assert response.status_code == 200
        assert response.text == "Success!"

        # Hanging request should have been retried on a different replica.
        assert len(ray.get(pid_tracker.get_pids.remote())) == 2

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
    ServeSubmissionClient("http://localhost:52365").deploy_applications(config)

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
    if RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING:
        assert response.status_code == 408
    else:
        assert response.status_code == 200
        assert response.text == "Success!"

    response = requests.get("http://localhost:8000")
    assert response.status_code == 200
    print("Requests succeeded! Deleting application.")
    ServeSubmissionClient("http://localhost:52365").delete_applications()


@pytest.mark.parametrize(
    "ray_instance",
    [
        {"RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0.1"},
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

    if RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING:
        # Streaming path does not retry on timeouts, so the requests should be failed.
        assert ray.get(response_ref1).status_code == 408
        assert ray.get(response_ref2).status_code == 408
        ray.get(signal_actor.send.remote())
        assert ray.get(do_request.remote()).status_code == 200
    else:
        # Legacy path retries on timeouts, so the requests should succeed.
        ray.get(signal_actor.send.remote())
        assert ray.get(response_ref1).status_code == 200
        assert ray.get(response_ref1).text == "Success!"
        assert ray.get(response_ref2).status_code == 200
        assert ray.get(response_ref2).text == "Success!"


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

    if RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING:
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
@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    reason="Only relevant on streaming codepath.",
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

    assert get_num_running_tasks() == 0

    # Send a number of requests that all will be timed out.
    results = ray.get([do_request.remote() for _ in range(10)])
    assert all(r.status_code == 408 for r in results)

    # The tasks should all be cancelled.
    wait_for_condition(lambda: get_num_running_tasks() == 0)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
