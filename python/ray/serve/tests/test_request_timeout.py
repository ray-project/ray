import asyncio
import os
import sys
from typing import Set

import pytest
import requests

import ray
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient

from ray import serve
from ray.serve.schema import ServeInstanceDetails
from ray.serve._private.common import ApplicationStatus
from ray.serve._private.constants import RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING


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
        assert response.status_code == 500
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
        assert response.status_code == 500
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
        assert ray.get(response_ref1).status_code == 500
        assert ray.get(response_ref2).status_code == 500
        ray.get(signal_actor.send.remote())
        assert ray.get(do_request.remote()).status_code == 200
    else:
        # Legacy path retries on timeouts, so the requests should succeed.
        ray.get(signal_actor.send.remote())
        assert ray.get(response_ref1).status_code == 200
        assert ray.get(response_ref1).text == "Success!"
        assert ray.get(response_ref2).status_code == 200
        assert ray.get(response_ref2).text == "Success!"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
