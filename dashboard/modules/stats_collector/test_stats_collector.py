import os
import sys
import logging
import requests
import time
import traceback

import pytest
import ray
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (
    format_web_url,
    wait_until_server_available,
)

os.environ["RAY_USE_NEW_DASHBOARD"] = "1"

logger = logging.getLogger(__name__)


def test_node_info(ray_start_with_dashboard):
    @ray.remote
    class Actor:
        def getpid(self):
            return os.getpid()

    actors = [Actor.remote(), Actor.remote()]
    actor_pids = [actor.getpid.remote() for actor in actors]
    actor_pids = set(ray.get(actor_pids))

    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    timeout_seconds = 10
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            response = requests.get(webui_url + "/nodes?view=hostnamelist")
            response.raise_for_status()
            hostname_list = response.json()
            assert hostname_list["result"] is True, hostname_list["msg"]
            hostname_list = hostname_list["data"]["hostNameList"]
            assert len(hostname_list) == 1

            hostname = hostname_list[0]
            response = requests.get(webui_url + f"/nodes/{hostname}")
            response.raise_for_status()
            detail = response.json()
            assert detail["result"] is True, detail["msg"]
            detail = detail["data"]["detail"]
            assert detail["hostname"] == hostname
            assert detail["state"] == "ALIVE"
            assert "raylet" in detail["cmdline"][0]
            assert len(detail["workers"]) >= 2
            assert len(detail["actors"]) == 2, detail["actors"]
            assert len(detail["raylet"]["viewData"]) > 0

            actor_worker_pids = set()
            for worker in detail["workers"]:
                if "ray::Actor" in worker["cmdline"][0]:
                    actor_worker_pids.add(worker["pid"])
            assert actor_worker_pids == actor_pids

            response = requests.get(webui_url + "/nodes?view=summary")
            response.raise_for_status()
            summary = response.json()
            assert summary["result"] is True, summary["msg"]
            assert len(summary["data"]["summary"]) == 1
            summary = summary["data"]["summary"][0]
            assert summary["hostname"] == hostname
            assert summary["state"] == "ALIVE"
            assert "raylet" in summary["cmdline"][0]
            assert "workers" not in summary
            assert "actors" not in summary
            assert "viewData" not in summary["raylet"]
            break
        except Exception as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = traceback.format_exception(
                    type(last_ex), last_ex,
                    last_ex.__traceback__) if last_ex else []
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
