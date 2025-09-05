import logging
import sys
import time
import traceback

import pytest
import requests

import ray
from ray._private.test_utils import format_web_url, wait_until_server_available
from ray.dashboard.modules.node import actor_consts
from ray.dashboard.tests.conftest import *  # noqa
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

logger = logging.getLogger(__name__)


def test_actors(disable_aiohttp_cache, ray_start_with_dashboard):
    """
    Tests the REST API dashboard calls on:
    - alive actors
    - infeasible actors
    - dead actors
    - pg actors (with pg_id set and required_resources formatted)
    """

    @ray.remote
    class Foo:
        def __init__(self, num):
            self.num = num

        def do_task(self):
            return self.num

        def get_node_id(self):
            return ray.get_runtime_context().get_node_id()

        def get_pid(self):
            import os

            return os.getpid()

        def __repr__(self) -> str:
            return "Foo1"

    @ray.remote(num_cpus=0, resources={"infeasible_actor": 1})
    class InfeasibleActor:
        pass

    pg = placement_group([{"CPU": 1}])

    @ray.remote(num_cpus=1)
    class PgActor:
        def __init__(self):
            pass

        def do_task(self):
            return 1

        def get_placement_group_id(self):
            return ray.get_runtime_context().get_placement_group_id()

    foo_actors = [Foo.options(name="first").remote(4), Foo.remote(5)]
    infeasible_actor = InfeasibleActor.options(name="infeasible").remote()  # noqa
    dead_actor = Foo.options(name="dead").remote(1)
    pg_actor = PgActor.options(
        name="pg",
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
        ),
    ).remote()

    ray.kill(dead_actor)
    [actor.do_task.remote() for actor in foo_actors]
    pg_actor.do_task.remote()
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)
    job_id = ray.get_runtime_context().get_job_id()
    node_id = ray.get(foo_actors[0].get_node_id.remote())
    pid = ray.get(foo_actors[0].get_pid.remote())
    placement_group_id = ray.get(pg_actor.get_placement_group_id.remote())

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
            assert len(actors) == 5

            for a in actors.values():
                if a["name"] == "first":
                    actor_response = a
            assert actor_response["jobId"] == job_id
            assert "Foo" in actor_response["className"]
            assert "address" in actor_response
            assert type(actor_response["address"]) is dict
            assert actor_response["address"]["nodeId"] == node_id
            assert actor_response["state"] == "ALIVE"
            assert actor_response["name"] == "first"
            assert actor_response["numRestarts"] == "0"
            assert actor_response["pid"] == pid
            assert actor_response["startTime"] > 0
            assert actor_response["requiredResources"] == {}
            assert actor_response["endTime"] == 0
            assert actor_response["exitDetail"] == "-"
            assert actor_response["reprName"] == "Foo1"
            for a in actors.values():
                # "exitDetail always exits from the response"
                assert "exitDetail" in a

            # Check the dead actor metadata.
            for a in actors.values():
                if a["name"] == "dead":
                    dead_actor_response = a
            assert dead_actor_response["endTime"] > 0
            assert "ray.kill" in dead_actor_response["exitDetail"]
            assert dead_actor_response["state"] == "DEAD"
            assert dead_actor_response["name"] == "dead"

            # Check the infeasible actor metadata.
            for a in actors.values():
                if a["name"] == "infeasible":
                    infeasible_actor_response = a
            # Make sure the infeasible actor's resource name is correct.
            assert infeasible_actor_response["requiredResources"] == {
                "infeasible_actor": 1
            }
            assert infeasible_actor_response["pid"] == 0
            all_pids = {entry["pid"] for entry in actors.values()}
            assert 0 in all_pids  # The infeasible actor
            assert len(all_pids) > 1

            # Check the pg actor metadata.
            for a in actors.values():
                if a["name"] == "pg":
                    pg_actor_response = a
            assert pg_actor_response["placementGroupId"] == placement_group_id
            assert pg_actor_response["requiredResources"] == {"CPU": 1.0}

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


def test_actor_with_ids(disable_aiohttp_cache, ray_start_with_dashboard):
    """
    Tests the REST API dashboard calls with actor ids in the URL
    """

    @ray.remote
    class Actor:
        def __init__(self, num):
            self.num = num

        def do_task(self):
            return self.num

        def get_actor_id(self):
            return ray.get_runtime_context().get_actor_id()

    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)
    actors = [Actor.options(name=f"Actor{i}").remote(i) for i in range(5)]
    for actor in actors:
        actor.do_task.remote()

    actor_ids = [ray.get(actor.get_actor_id.remote()) for actor in actors]

    timeout_seconds = 5
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            actor_idx = 2
            resp = requests.get(f"{webui_url}/logical/actors/{actor_ids[actor_idx]}")
            resp_json = resp.json()
            resp_data = resp_json["data"]
            actor_detail = resp_data["detail"]
            assert actor_detail["state"] == "ALIVE"
            assert actor_detail["name"] == f"Actor{actor_idx}"
            assert actor_detail["numRestarts"] == "0"
            assert actor_detail["startTime"] > 0
            assert actor_detail["requiredResources"] == {}
            assert actor_detail["endTime"] == 0
            assert actor_detail["exitDetail"] == "-"

            actor_idxs = [0, 1, 4]
            actor_idxs_to_id_str = ",".join([str(actor_ids[i]) for i in actor_idxs])
            resp = requests.get(
                f"{webui_url}/logical/actors?ids={actor_idxs_to_id_str}"
            )
            resp_json = resp.json()
            resp_actors = resp_json["data"]["actors"]
            assert len(resp_actors) == len(actor_idxs)
            for actor_idx in actor_idxs:
                actor_detail = resp_actors[str(actor_ids[actor_idx])]
                assert actor_detail["state"] == "ALIVE"
                assert actor_detail["name"] == f"Actor{actor_idx}"
                assert actor_detail["numRestarts"] == "0"
                assert actor_detail["startTime"] > 0
                assert actor_detail["requiredResources"] == {}
                assert actor_detail["endTime"] == 0
                assert actor_detail["exitDetail"] == "-"

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


def test_nil_node(enable_test_module, disable_aiohttp_cache, ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
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
            assert actor_consts.NIL_NODE_ID not in result["data"]["nodeActors"]
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


def test_actor_cleanup(
    disable_aiohttp_cache, reduce_actor_cache, ray_start_with_dashboard
):
    @ray.remote
    class Foo:
        def __init__(self, num):
            self.num = num

        def do_task(self):
            return self.num

    @ray.remote(num_gpus=1)
    class InfeasibleActor:
        pass

    infeasible_actor = InfeasibleActor.remote()  # noqa

    foo_actors = [
        Foo.remote(1),
        Foo.remote(2),
        Foo.remote(3),
        Foo.remote(4),
        Foo.remote(5),
        Foo.remote(6),
    ]
    results = [actor.do_task.remote() for actor in foo_actors]  # noqa
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    timeout_seconds = 8
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            resp = requests.get(f"{webui_url}/logical/actors")
            resp_json = resp.json()
            resp_data = resp_json["data"]
            actors = resp_data["actors"]
            # Although max cache is 3, there should be 7 actors
            # because they are all still alive.
            assert len(actors) == 7

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

    # kill
    ray.kill(infeasible_actor)
    [ray.kill(foo_actor) for foo_actor in foo_actors]
    # Wait 5 seconds for cleanup to finish
    time.sleep(5)

    # Check only three remaining in cache
    start_time = time.time()
    while True:
        time.sleep(1)
        try:
            resp = requests.get(f"{webui_url}/logical/actors")
            resp_json = resp.json()
            resp_data = resp_json["data"]
            actors = resp_data["actors"]
            # Max cache is 3 so only 3 actors should be left.
            assert len(actors) == 3

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
