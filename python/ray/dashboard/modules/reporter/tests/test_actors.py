import logging
import os
import sys
import time

import pytest
import requests

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.state_api_test_utils import _is_actor_task_running
from ray._private.test_utils import format_web_url, wait_until_server_available
from ray.dashboard.tests.conftest import *  # noqa

import psutil

logger = logging.getLogger(__name__)

KILL_ACTOR_ENDPOINT = "/api/actors/kill"


def _actor_killed(pid: str) -> bool:
    """Check if a process with given pid is running."""
    return not psutil.pid_exists(int(pid))


def _kill_actor_using_dashboard_gcs(
    webui_url: str, actor_id: str, expected_status_code: int, force_kill=False
):
    resp = requests.get(
        webui_url + KILL_ACTOR_ENDPOINT,
        params={
            "actor_id": actor_id,
            "force_kill": force_kill,
        },
        timeout=5,
    )
    assert resp.status_code == expected_status_code
    resp_json = resp.json()
    return resp_json


@pytest.mark.parametrize("enable_concurrency_group", [False, True])
def test_kill_actor_gcs(ray_start_with_dashboard, enable_concurrency_group):
    # Start the dashboard
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    concurrency_groups = {"io": 1} if enable_concurrency_group else None

    @ray.remote(concurrency_groups=concurrency_groups)
    class Actor:
        def f(self):
            ray._private.worker.show_in_dashboard("test")
            return os.getpid()

        def loop(self):
            while True:
                time.sleep(1)
                print("Looping...")

    # Create an actor
    a = Actor.remote()
    worker_pid = ray.get(a.f.remote())  # noqa
    actor_id = a._ray_actor_id.hex()

    OK = 200
    NOT_FOUND = 404

    # Kill a non-existent actor
    resp = _kill_actor_using_dashboard_gcs(
        webui_url, "non-existent-actor-id", NOT_FOUND
    )
    assert "not found" in resp["msg"]

    # Kill the actor
    resp = _kill_actor_using_dashboard_gcs(webui_url, actor_id, OK, force_kill=False)
    assert "It will exit once running tasks complete" in resp["msg"]
    wait_for_condition(lambda: _actor_killed(worker_pid))

    # Create an actor and have it loop
    a = Actor.remote()
    worker_pid = ray.get(a.f.remote())  # noqa
    actor_id = a._ray_actor_id.hex()
    a.loop.remote()

    # wait for loop() to start
    wait_for_condition(lambda: _is_actor_task_running(worker_pid, "Actor.loop"))

    # Try to kill the actor, it should not die since a task is running
    resp = _kill_actor_using_dashboard_gcs(webui_url, actor_id, OK, force_kill=False)
    assert "It will exit once running tasks complete" in resp["msg"]
    with pytest.raises(
        RuntimeError, match="The condition wasn't met before the timeout expired."
    ):
        wait_for_condition(lambda: _actor_killed(worker_pid), 1)

    # Force kill the actor
    resp = _kill_actor_using_dashboard_gcs(webui_url, actor_id, OK, force_kill=True)
    assert "Force killed actor with id" in resp["msg"]
    wait_for_condition(lambda: _actor_killed(worker_pid))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
