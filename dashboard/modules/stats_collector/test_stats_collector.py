import os
import logging
import requests
import time

import ray
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (
    wait_until_server_available, )

os.environ["RAY_USE_NEW_DASHBOARD"] = "1"

logger = logging.getLogger(__name__)


def test_node_info(ray_start_with_dashboard):
    @ray.remote
    class Actor:
        def getpid(self):
            return os.getpid()

    actors = [Actor.remote(), Actor.remote()]
    actor_pids = [actor.getpid.remote() for actor in actors]
    actor_pids = ray.get(actor_pids)

    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = webui_url.replace("localhost", "http://127.0.0.1")

    timeout_seconds = 20
    start_time = time.time()
    while True:
        time.sleep(1)
        try:
            response = requests.get(webui_url + "/nodes?view=hostnamelist")
            response.raise_for_status()
            hostname_list = response.json()
            assert hostname_list["result"] is True
            hostname_list = hostname_list["data"]["hostNameList"]
            assert len(hostname_list) == 1

            hostname = hostname_list[0]
            response = requests.get(webui_url + "/nodes/{}".format(hostname))
            response.raise_for_status()
            detail = response.json()
            assert detail["result"] is True
            detail = detail["data"]["detail"]
            assert detail["hostname"] == hostname
            assert detail["state"] == "ALIVE"
            assert "raylet" in detail["cmdline"][0]
            assert len(detail["workers"]) >= 2
            assert len(detail["actors"]) == 2
            assert len(detail["raylet"]["viewData"]) > 0

            response = requests.get(webui_url + "/nodes?view=summary")
            response.raise_for_status()
            summary = response.json()
            assert summary["result"] is True
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
            logger.info(ex)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception(
                    "Timed out while waiting for dashboard to start.")
