import os
import sys
import logging
import requests
import time
import traceback
import random
import pytest
import ray
import redis
import threading
import ray.new_dashboard.modules.stats_collector.stats_collector_consts \
    as stats_collector_consts
import ray.new_dashboard.utils as dashboard_utils
import ray.ray_constants as ray_constants
from datetime import datetime, timedelta
from ray.cluster_utils import Cluster
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (format_web_url, wait_until_server_available,
                            wait_for_condition,
                            wait_until_succeeded_without_exception)

logger = logging.getLogger(__name__)


def test_node_info(disable_aiohttp_cache, ray_start_with_dashboard):
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
    node_id = ray_start_with_dashboard["node_id"]

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
            response = requests.get(webui_url + f"/nodes/{node_id}")
            response.raise_for_status()
            detail = response.json()
            assert detail["result"] is True, detail["msg"]
            detail = detail["data"]["detail"]
            assert detail["hostname"] == hostname
            assert detail["raylet"]["state"] == "ALIVE"
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
            assert summary["raylet"]["state"] == "ALIVE"
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


def test_memory_table(disable_aiohttp_cache, ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"]))

    @ray.remote
    class ActorWithObjs:
        def __init__(self):
            self.obj_ref = ray.put([1, 2, 3])

        def get_obj(self):
            return ray.get(self.obj_ref)

    my_obj = ray.put([1, 2, 3] * 100)  # noqa
    actors = [ActorWithObjs.remote() for _ in range(2)]  # noqa
    results = ray.get([actor.get_obj.remote() for actor in actors])  # noqa
    webui_url = format_web_url(ray_start_with_dashboard["webui_url"])
    resp = requests.get(
        webui_url + "/memory/set_fetch", params={"shouldFetch": "true"})
    resp.raise_for_status()

    def check_mem_table():
        resp = requests.get(f"{webui_url}/memory/memory_table")
        resp_data = resp.json()
        assert resp_data["result"]
        latest_memory_table = resp_data["data"]["memoryTable"]
        summary = latest_memory_table["summary"]
        # 1 ref per handle and per object the actor has a ref to
        assert summary["totalActorHandles"] == len(actors) * 2
        # 1 ref for my_obj
        assert summary["totalLocalRefCount"] == 1

    wait_until_succeeded_without_exception(
        check_mem_table, (AssertionError, ), timeout_ms=1000)


def test_get_all_node_details(disable_aiohttp_cache, ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"]))

    webui_url = format_web_url(ray_start_with_dashboard["webui_url"])

    @ray.remote
    class ActorWithObjs:
        def __init__(self):
            print("I also log a line")
            self.obj_ref = ray.put([1, 2, 3])

        def get_obj(self):
            return ray.get(self.obj_ref)

    actors = [ActorWithObjs.remote() for _ in range(2)]  # noqa
    timeout_seconds = 20
    start_time = time.time()
    last_ex = None

    def check_node_details():
        resp = requests.get(f"{webui_url}/nodes?view=details")
        resp_json = resp.json()
        resp_data = resp_json["data"]
        clients = resp_data["clients"]
        node = clients[0]
        assert len(clients) == 1
        assert len(node.get("actors")) == 2
        # Workers information should be in the detailed payload
        assert "workers" in node
        assert "logCount" in node
        # Two lines printed by ActorWithObjs
        # One line printed by autoscaler: monitor.py:118 -- Monitor: Started
        assert node["logCount"] > 2
        print(node["workers"])
        assert len(node["workers"]) == 2
        assert node["workers"][0]["logCount"] == 1

    while True:
        time.sleep(1)
        try:
            check_node_details()
            break
        except (AssertionError, KeyError, IndexError) as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = traceback.format_exception(
                    type(last_ex), last_ex,
                    last_ex.__traceback__) if last_ex else []
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "include_dashboard": True
    }], indirect=True)
def test_multi_nodes_info(enable_test_module, disable_aiohttp_cache,
                          ray_start_cluster_head):
    cluster: Cluster = ray_start_cluster_head
    assert (wait_until_server_available(cluster.webui_url) is True)
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)
    cluster.add_node()
    cluster.add_node()

    def _check_nodes():
        try:
            response = requests.get(webui_url + "/nodes?view=summary")
            response.raise_for_status()
            summary = response.json()
            assert summary["result"] is True, summary["msg"]
            summary = summary["data"]["summary"]
            assert len(summary) == 3
            for node_info in summary:
                node_id = node_info["raylet"]["nodeId"]
                response = requests.get(webui_url + f"/nodes/{node_id}")
                response.raise_for_status()
                detail = response.json()
                assert detail["result"] is True, detail["msg"]
                detail = detail["data"]["detail"]
                assert detail["raylet"]["state"] == "ALIVE"
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
    "ray_start_cluster_head", [{
        "include_dashboard": True
    }], indirect=True)
def test_multi_node_churn(enable_test_module, disable_aiohttp_cache,
                          ray_start_cluster_head):
    cluster: Cluster = ray_start_cluster_head
    assert (wait_until_server_available(cluster.webui_url) is True)
    webui_url = format_web_url(cluster.webui_url)

    def cluster_chaos_monkey():
        worker_nodes = []
        while True:
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

    def get_index():
        resp = requests.get(webui_url)
        resp.raise_for_status()

    def get_nodes():
        resp = requests.get(webui_url + "/nodes?view=summary")
        resp.raise_for_status()
        summary = resp.json()
        assert summary["result"] is True, summary["msg"]
        assert summary["data"]["summary"]

    t = threading.Thread(target=cluster_chaos_monkey, daemon=True)
    t.start()

    t_st = datetime.now()
    duration = timedelta(seconds=60)
    while datetime.now() < t_st + duration:
        get_index()
        time.sleep(2)


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "include_dashboard": True
    }], indirect=True)
def test_logs(enable_test_module, disable_aiohttp_cache,
              ray_start_cluster_head):
    cluster = ray_start_cluster_head
    assert (wait_until_server_available(cluster.webui_url) is True)
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)
    nodes = ray.nodes()
    assert len(nodes) == 1
    node_ip = nodes[0]["NodeManagerAddress"]

    @ray.remote
    class LoggingActor:
        def go(self, n):
            i = 0
            while i < n:
                print(f"On number {i}")
                i += 1

        def get_pid(self):
            return os.getpid()

    la = LoggingActor.remote()
    la2 = LoggingActor.remote()
    la_pid = str(ray.get(la.get_pid.remote()))
    la2_pid = str(ray.get(la2.get_pid.remote()))
    ray.get(la.go.remote(4))
    ray.get(la2.go.remote(1))

    def check_logs():
        node_logs_response = requests.get(
            f"{webui_url}/node_logs", params={"ip": node_ip})
        node_logs_response.raise_for_status()
        node_logs = node_logs_response.json()
        assert node_logs["result"]
        assert type(node_logs["data"]["logs"]) is dict
        assert all(
            pid in node_logs["data"]["logs"] for pid in (la_pid, la2_pid))
        assert len(node_logs["data"]["logs"][la2_pid]) == 1

        actor_one_logs_response = requests.get(
            f"{webui_url}/node_logs",
            params={
                "ip": node_ip,
                "pid": str(la_pid)
            })
        actor_one_logs_response.raise_for_status()
        actor_one_logs = actor_one_logs_response.json()
        assert actor_one_logs["result"]
        assert type(actor_one_logs["data"]["logs"]) is dict
        assert len(actor_one_logs["data"]["logs"][la_pid]) == 4

    wait_until_succeeded_without_exception(
        check_logs, (AssertionError), timeout_ms=1000)


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "include_dashboard": True
    }], indirect=True)
def test_errors(enable_test_module, disable_aiohttp_cache,
                ray_start_cluster_head):
    cluster = ray_start_cluster_head
    assert (wait_until_server_available(cluster.webui_url) is True)
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)
    nodes = ray.nodes()
    assert len(nodes) == 1
    node_ip = nodes[0]["NodeManagerAddress"]

    @ray.remote
    class ErrorActor():
        def go(self):
            raise ValueError("This is an error")

        def get_pid(self):
            return os.getpid()

    ea = ErrorActor.remote()
    ea_pid = ea.get_pid.remote()
    ea.go.remote()

    def check_errs():
        node_errs_response = requests.get(
            f"{webui_url}/node_logs", params={"ip": node_ip})
        node_errs_response.raise_for_status()
        node_errs = node_errs_response.json()
        assert node_errs["result"]
        assert type(node_errs["data"]["errors"]) is dict
        assert ea_pid in node_errs["data"]["errors"]
        assert len(node_errs["data"]["errors"][ea_pid]) == 1

        actor_err_response = requests.get(
            f"{webui_url}/node_logs",
            params={
                "ip": node_ip,
                "pid": str(ea_pid)
            })
        actor_err_response.raise_for_status()
        actor_errs = actor_err_response.json()
        assert actor_errs["result"]
        assert type(actor_errs["data"]["errors"]) is dict
        assert len(actor_errs["data"]["errors"][ea_pid]) == 4

    wait_until_succeeded_without_exception(
        check_errs, (AssertionError), timeout_ms=1000)


def test_nil_node(enable_test_module, disable_aiohttp_cache,
                  ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    @ray.remote(num_gpus=1)
    class InfeasibleActor:
        pass

    infeasible_actor = InfeasibleActor.remote()  # noqa

    timeout_seconds = 5
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            resp = requests.get(f"{webui_url}/logical/actors")
            resp_json = resp.json()
            resp_data = resp_json["data"]
            actors = resp_data["actors"]
            assert len(actors) == 1
            response = requests.get(webui_url + "/test/dump?key=node_actors")
            response.raise_for_status()
            result = response.json()
            assert stats_collector_consts.NIL_NODE_ID not in result["data"][
                "nodeActors"]
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


def test_actor_pubsub(disable_aiohttp_cache, ray_start_with_dashboard):
    timeout = 5
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    address_info = ray_start_with_dashboard
    address = address_info["redis_address"]
    address = address.split(":")
    assert len(address) == 2

    client = redis.StrictRedis(
        host=address[0],
        port=int(address[1]),
        password=ray_constants.REDIS_DEFAULT_PASSWORD)

    p = client.pubsub(ignore_subscribe_messages=True)
    p.psubscribe(ray.gcs_utils.RAY_ACTOR_PUBSUB_PATTERN)

    @ray.remote
    class DummyActor:
        def __init__(self):
            pass

    # Create a dummy actor.
    a = DummyActor.remote()

    def handle_pub_messages(client, msgs, timeout, expect_num):
        start_time = time.time()
        while time.time() - start_time < timeout and len(msgs) < expect_num:
            msg = client.get_message()
            if msg is None:
                time.sleep(0.01)
                continue
            pubsub_msg = ray.gcs_utils.PubSubMessage.FromString(msg["data"])
            actor_data = ray.gcs_utils.ActorTableData.FromString(
                pubsub_msg.data)
            msgs.append(actor_data)

    msgs = []
    handle_pub_messages(p, msgs, timeout, 2)

    # Assert we received published actor messages with state
    # DEPENDENCIES_UNREADY and ALIVE.
    assert len(msgs) == 2

    # Kill actor.
    ray.kill(a)
    handle_pub_messages(p, msgs, timeout, 3)

    # Assert we received published actor messages with state DEAD.
    assert len(msgs) == 3

    def actor_table_data_to_dict(message):
        return dashboard_utils.message_to_dict(
            message, {
                "actorId", "parentId", "jobId", "workerId", "rayletId",
                "actorCreationDummyObjectId", "callerId", "taskId",
                "parentTaskId", "sourceActorId", "placementGroupId"
            },
            including_default_value_fields=False)

    non_state_keys = ("actorId", "jobId", "taskSpec")
    for msg in msgs:
        actor_data_dict = actor_table_data_to_dict(msg)
        # DEPENDENCIES_UNREADY is 0, which would not be keeped in dict. We
        # need check its original value.
        if msg.state == 0:
            assert len(actor_data_dict) > 5
            for k in non_state_keys:
                assert k in actor_data_dict
        # For status that is not DEPENDENCIES_UNREADY, only states fields will
        # be published.
        elif actor_data_dict["state"] in ("ALIVE", "DEAD"):
            assert actor_data_dict.keys() == {
                "state", "address", "timestamp", "pid", "creationTaskException"
            }
        else:
            raise Exception("Unknown state: {}".format(
                actor_data_dict["state"]))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
