import sys

import httpx
import pytest
from starlette.requests import Request

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import RAY_SERVE_ENABLE_TASK_EVENTS
from ray.util.state import list_tasks


@ray.remote
def some_task():
    return "hi from task"


@serve.deployment
class Deployment:
    async def __call__(self, request: Request) -> str:
        if (await request.json())["call_task"] is True:
            msg = await some_task.remote()
        else:
            msg = "hi from deployment"

        return msg


@pytest.mark.skipif(RAY_SERVE_ENABLE_TASK_EVENTS, reason="Task events enabled.")
def test_task_events_disabled_by_default(serve_instance):
    # Deploy simple Serve app and call it over HTTP.
    # Check that none of the Serve-managed actors or tasks generate events.
    serve.run(Deployment.bind())

    assert (
        httpx.request("GET", "http://localhost:8000/", json={"call_task": False}).text
        == "hi from deployment"
    )
    for _ in range(100):
        assert len(list_tasks()) == 0

    # Now call a Ray task from within the deployment.
    # A task event should be generated.
    assert (
        httpx.request("GET", "http://localhost:8000/", json={"call_task": True}).text
        == "hi from task"
    )
    wait_for_condition(lambda: len(list_tasks()) == 1)
    [task_state] = list_tasks()
    assert task_state.name == "some_task"
    assert task_state.type == "NORMAL_TASK"


@pytest.mark.skipif(not RAY_SERVE_ENABLE_TASK_EVENTS, reason="Task events disabled.")
def test_enable_task_events(serve_instance):
    # Deploy simple Serve app and call it over HTTP.
    # Check that the Serve-managed actors generate events.
    serve.run(Deployment.bind())

    assert (
        httpx.request("GET", "http://localhost:8000/", json={"call_task": False}).text
        == "hi from deployment"
    )

    def check_for_expected_actor_tasks():
        found_proxy = False
        found_replica = False
        found_controller = False
        for task_state in list_tasks():
            if "ProxyActor" in task_state.name:
                found_proxy = True
            if "ServeReplica" in task_state.name:
                found_replica = True
            if "ServeController" in task_state.name:
                found_controller = True

        assert found_proxy, "No proxy tasks found."
        assert found_replica, "No replica tasks found."
        assert found_controller, "No controller tasks found."
        return True

    wait_for_condition(check_for_expected_actor_tasks, timeout=5)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
