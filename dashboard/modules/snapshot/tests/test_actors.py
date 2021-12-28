import os
import sys
import logging
import requests
import time
import ray
import pytest
from ray.dashboard.tests.conftest import *  # noqa
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
)

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


def _kill_actor_using_dashboard_gcs(webui_url: str, actor_id: str, force_kill=False):
    resp = requests.get(
        webui_url + KILL_ACTOR_ENDPOINT,
        params={
            "actor_id": actor_id,
            "force_kill": force_kill,
        },
    )
    resp.raise_for_status()
    resp_json = resp.json()
    assert resp_json["result"] is True, "msg" in resp_json
    return resp_json


def test_kill_actor_gcs(ray_start_with_dashboard):
    # Start the dashboard
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    @ray.remote
    class Actor:
        def f(self):
            ray.worker.show_in_dashboard("test")
            return os.getpid()

        def loop(self):
            while True:
                time.sleep(1)
                print("Looping...")

    # Create an actor
    a = Actor.remote()
    worker_pid = ray.get(a.f.remote())  # noqa
    actor_id = a._ray_actor_id.hex()

    # Kill the actor
    _kill_actor_using_dashboard_gcs(webui_url, actor_id, force_kill=False)
    assert _actor_killed_loop(worker_pid)

    # Create an actor and have it loop
    a = Actor.remote()
    worker_pid = ray.get(a.f.remote())  # noqa
    actor_id = a._ray_actor_id.hex()
    a.loop.remote()

    # Try to kill the actor, it should not die since a task is running
    _kill_actor_using_dashboard_gcs(webui_url, actor_id, force_kill=False)
    assert not _actor_killed_loop(worker_pid, timeout_secs=1)

    # Force kill the actor
    _kill_actor_using_dashboard_gcs(webui_url, actor_id, force_kill=True)
    assert _actor_killed_loop(worker_pid)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
