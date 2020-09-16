import os
import sys
import logging
import requests
import time
import traceback

import pytest
import ray
from ray.new_dashboard.tests.conftest import *  # noqa
from datetime import datetime, timedelta
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


def test_memory_table(ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"]))

    @ray.remote
    class ActorWithObjs:
        def __init__(self):
            self.obj_ref = ray.put([1,2,3])

        def get_obj(self):
            return ray.get(self.obj_ref)

    my_obj = ray.put([1,2,3] * 100)
    actors = [ActorWithObjs.remote() for _ in range(2)]
    results = ray.get([actor.get_obj.remote() for actor in actors])
    webui_url = format_web_url(ray_start_with_dashboard["webui_url"])
    resp = requests.post(webui_url + "/memory/set_fetch", json={"shouldFetch": "true"})
    resp.raise_for_status()

    memory_fetch_threshhold = datetime.now() + timedelta(seconds=5)
    done_in_time = False
    latest_memory_table = None
    while datetime.now() < memory_fetch_threshhold:
        resp = requests.get(f"{webui_url}/memory/memory_table")
        if not resp or "data" not in resp.json():
            time.sleep(.5)
            continue
        resp_data = resp.json()["data"]
        latest_memory_table = resp_data["memoryTable"]
        summary = latest_memory_table["summary"]
        try:
            # 1 ref per handle and per object the actor has a ref to
            assert summary["totalActorHandles"] == len(actors) * 2 
            # 1 ref for my_obj
            assert summary["totalLocalRefCount"] == 1
        except AssertionError as e:
            time.sleep(.5)
            continue
        done_in_time = True
        break
    assert done_in_time

def test_get_all_node_details(ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"]))

    webui_url = format_web_url(ray_start_with_dashboard["webui_url"])
    @ray.remote
    class ActorWithObjs:
        def __init__(self):
            self.obj_ref = ray.put([1,2,3])

        def get_obj(self):
            return ray.get(self.obj_ref)

    my_obj = ray.put([1,2,3] * 100)
    actors = [ActorWithObjs.remote() for _ in range(2)]

    threshhold = datetime.now() + timedelta(seconds=5)
    done_in_time = False
    while datetime.now() < threshhold:
        resp = requests.get(f"{webui_url}/nodes?view=details")
        if not resp or "data" not in resp.json():
            time.sleep(.5)
            continue
        resp_data = resp.json()["data"]
        print(resp_data)
        clients = resp_data["clients"]
        logger.warning(clients)
        try:
            assert len(clients) == 1
            assert len(clients[0].get('actors')) == 2
        except AssertionError as e:
            time.sleep(.5)
            continue
        done_in_time = True
        break
    assert done_in_time

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
