import logging
import os
import sys
import time

import pytest
import requests

import ray
from ray._private.test_utils import format_web_url, wait_until_server_available
from ray.dashboard.tests.conftest import *  # noqa

logger = logging.getLogger(__name__)

KILL_ACTOR_ENDPOINT = "/api/actors/kill"


def _actor_killed(pid: str) -> bool:
    """Check For the existence of a unix pid."""
    try:
        os.kill(pid, 0)
    except OSError:
        return True
    else:
        return False


def _actor_killed_loop(worker_pid: str, timeout_secs=3) -> bool:
    dead = False
    for _ in range(timeout_secs):
        time.sleep(1)
        if _actor_killed(worker_pid):
            dead = True
            break
    return dead


def _kill_actor_using_dashboard_gcs(
    webui_url: str, actor_id: str, expected_status_code: int, force_kill=False
):
    resp = requests.get(
        webui_url + KILL_ACTOR_ENDPOINT,
        params={
            "actor_id": actor_id,
            "force_kill": force_kill,
        },
    )
    assert resp.status_code == expected_status_code
    resp_json = resp.json()
    return resp_json


def test_kill_actor_gcs(ray_start_with_dashboard):
    # Start the dashboard
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    @ray.remote
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
    INTERNAL_ERROR = 500

    # Kill an non-existent actor
    # TODO(kevin85421): It should return 404 instead of 500.
    resp = _kill_actor_using_dashboard_gcs(
        webui_url, "non-existent-actor-id", INTERNAL_ERROR
    )
    assert "not found" in resp["msg"]

    # Kill the actor
    resp = _kill_actor_using_dashboard_gcs(webui_url, actor_id, OK, force_kill=False)
    assert "It will exit once running tasks complete" in resp["msg"]
    assert _actor_killed_loop(worker_pid)

    # Create an actor and have it loop
    a = Actor.remote()
    worker_pid = ray.get(a.f.remote())  # noqa
    actor_id = a._ray_actor_id.hex()
    a.loop.remote()

    # Try to kill the actor, it should not die since a task is running
    resp = _kill_actor_using_dashboard_gcs(webui_url, actor_id, OK, force_kill=False)
    assert "It will exit once running tasks complete" in resp["msg"]
    assert not _actor_killed_loop(worker_pid, timeout_secs=1)

    # Force kill the actor
    resp = _kill_actor_using_dashboard_gcs(webui_url, actor_id, OK, force_kill=True)
    assert "Force killed actor with id" in resp["msg"]
    assert _actor_killed_loop(worker_pid)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
