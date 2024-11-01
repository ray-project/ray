import logging
import os
import random
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta

import pytest
import requests

import ray
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray.cluster_utils import Cluster
from ray.dashboard.consts import RAY_DASHBOARD_STATS_UPDATING_INTERVAL
from ray.dashboard.tests.conftest import *  # noqa

logger = logging.getLogger(__name__)


def test_nodes_update(enable_test_module, ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    timeout_seconds = 10
    start_time = time.time()
    while True:
        time.sleep(1)
        try:
            response = requests.get(webui_url + "/test/dump")
            response.raise_for_status()
            try:
                dump_info = response.json()
            except Exception as ex:
                logger.info("failed response: %s", response.text)
                raise ex
            assert dump_info["result"] is True
            dump_data = dump_info["data"]
            assert len(dump_data["nodes"]) == 1
            assert len(dump_data["agents"]) == 1

            response = requests.get(webui_url + "/test/notified_agents")
            response.raise_for_status()
            try:
                notified_agents = response.json()
            except Exception as ex:
                logger.info("failed response: %s", response.text)
                raise ex
            assert notified_agents["result"] is True
            notified_agents = notified_agents["data"]
            assert len(notified_agents) == 1
            assert notified_agents == dump_data["agents"]
            break
        except (AssertionError, requests.exceptions.ConnectionError) as e:
            logger.info("Retry because of %s", e)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception("Timed out while testing.")


def test_node_info(disable_aiohttp_cache, ray_start_with_dashboard):
    @ray.remote
    class Actor:
        def getpid(self):
            print(f"actor pid={os.getpid()}")
            return os.getpid()

    actors = [Actor.remote(), Actor.remote()]
    actor_pids = [actor.getpid.remote() for actor in actors]
    actor_pids = set(ray.get(actor_pids))

    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = ray_start_with_dashboard["node_id"]

    # NOTE: Leaving sum buffer time for data to get refreshed
    timeout_seconds = RAY_DASHBOARD_STATS_UPDATING_INTERVAL * 1.5

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
            response = requests.get(webui_url + f"/nodes/{node_id}")
            response.raise_for_status()
            detail = response.json()
            assert detail["result"] is True, detail["msg"]
            detail = detail["data"]["detail"]
            assert detail["hostname"] == hostname
            assert detail["raylet"]["state"] == "ALIVE"
            assert detail["raylet"]["isHeadNode"] is True
            assert "raylet" in detail["cmdline"][0]
            assert len(detail["workers"]) >= 2
            assert len(detail["actors"]) == 2, detail["actors"]

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
            assert summary["raylet"]["state"] == "ALIVE"
            assert "raylet" in summary["cmdline"][0]
            assert "workers" not in summary
            assert "actors" not in summary
            assert "objectStoreAvailableMemory" in summary["raylet"]
            assert "objectStoreUsedMemory" in summary["raylet"]
            break
        except Exception as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = (
                    traceback.format_exception(
                        type(last_ex), last_ex, last_ex.__traceback__
                    )
                    if last_ex
                    else []
                )
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "include_dashboard": True,
            "_system_config": {
                "health_check_initial_delay_ms": 0,
                "health_check_timeout_ms": 100,
                "health_check_failure_threshold": 3,
                "health_check_period_ms": 100,
            },
        }
    ],
    indirect=True,
)
def test_multi_nodes_info(
    enable_test_module, disable_aiohttp_cache, ray_start_cluster_head
):
    cluster: Cluster = ray_start_cluster_head
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)
    cluster.add_node()
    cluster.add_node()
    dead_node = cluster.add_node()
    cluster.remove_node(dead_node, allow_graceful=False)

    def _check_nodes():
        try:
            response = requests.get(webui_url + "/nodes?view=summary")
            response.raise_for_status()
            summary = response.json()
            assert summary["result"] is True, summary["msg"]
            summary = summary["data"]["summary"]
            assert len(summary) == 4
            for node_info in summary:
                node_id = node_info["raylet"]["nodeId"]
                response = requests.get(webui_url + f"/nodes/{node_id}")
                response.raise_for_status()
                detail = response.json()
                assert detail["result"] is True, detail["msg"]
                detail = detail["data"]["detail"]
                if node_id != dead_node.node_id:
                    assert detail["raylet"]["state"] == "ALIVE"
                else:
                    assert detail["raylet"]["state"] == "DEAD"
            response = requests.get(webui_url + "/test/dump?key=agents")
            response.raise_for_status()
            agents = response.json()
            assert len(agents["data"]["agents"]) == 3
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_nodes, timeout=15)


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{"include_dashboard": True}], indirect=True
)
def test_multi_node_churn(
    enable_test_module, disable_aiohttp_cache, ray_start_cluster_head
):
    cluster: Cluster = ray_start_cluster_head
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = format_web_url(cluster.webui_url)

    success = True

    def verify():
        nonlocal success
        while True:
            try:
                resp = requests.get(webui_url)
                resp.raise_for_status()
                resp = requests.get(webui_url + "/nodes?view=summary")
                resp.raise_for_status()
                summary = resp.json()
                assert summary["result"] is True, summary["msg"]
                assert summary["data"]["summary"]
                time.sleep(1)
            except Exception:
                success = False
                break

    t = threading.Thread(target=verify, daemon=True)
    t.start()

    t_st = datetime.now()
    duration = timedelta(seconds=60)
    worker_nodes = []
    while datetime.now() < t_st + duration:
        time.sleep(5)
        if len(worker_nodes) < 2:
            worker_nodes.append(cluster.add_node())
            continue
        should_add_node = random.randint(0, 1)
        if should_add_node:
            worker_nodes.append(cluster.add_node())
        else:
            node_index = random.randrange(0, len(worker_nodes))
            node_to_remove = worker_nodes.pop(node_index)
            cluster.remove_node(node_to_remove)

    assert success


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
