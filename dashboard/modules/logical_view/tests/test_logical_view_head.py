import os
import sys
import logging
import requests
import time
import traceback
import ray
import pytest
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (
    format_web_url,
    wait_until_server_available,
)

logger = logging.getLogger(__name__)


def test_actor_groups(ray_start_with_dashboard):
    @ray.remote
    class Foo:
        def __init__(self, num):
            self.num = num

        def do_task(self):
            return self.num

    @ray.remote(num_gpus=1)
    class InfeasibleActor:
        pass

    foo_actors = [Foo.remote(4), Foo.remote(5)]
    infeasible_actor = InfeasibleActor.remote()  # noqa
    results = [actor.do_task.remote() for actor in foo_actors]  # noqa
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    timeout_seconds = 10
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            response = requests.get(webui_url + "/logical/actor_groups")
            response.raise_for_status()
            actor_groups_resp = response.json()
            assert actor_groups_resp["result"] is True, actor_groups_resp[
                "msg"]
            actor_groups = actor_groups_resp["data"]["actorGroups"]
            assert "Foo" in actor_groups
            summary = actor_groups["Foo"]["summary"]
            # 2 __init__ tasks and 2 do_task tasks
            assert summary["numExecutedTasks"] == 4
            assert summary["stateToCount"]["ALIVE"] == 2

            entries = actor_groups["Foo"]["entries"]
            foo_entry = entries[0]
            assert type(foo_entry["gpus"]) is list
            assert "timestamp" in foo_entry
            assert "actorConstructor" in foo_entry
            assert "actorClass" in foo_entry
            assert "actorId" in foo_entry
            assert "ipAddress" in foo_entry
            assert len(entries) == 2
            assert "InfeasibleActor" in actor_groups

            entries = actor_groups["InfeasibleActor"]["entries"]
            assert "requiredResources" in entries[0]
            assert "GPU" in entries[0]["requiredResources"]
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


def test_actors(disable_aiohttp_cache, ray_start_with_dashboard):
    @ray.remote
    class Foo:
        def __init__(self, num):
            self.num = num

        def do_task(self):
            return self.num

    @ray.remote(num_gpus=1)
    class InfeasibleActor:
        pass

    foo_actors = [Foo.remote(4), Foo.remote(5)]
    infeasible_actor = InfeasibleActor.remote()  # noqa
    results = [actor.do_task.remote() for actor in foo_actors]  # noqa
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

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
            assert len(actors) == 3
            one_entry = list(actors.values())[0]
            assert "jobId" in one_entry
            assert "taskSpec" in one_entry
            assert "functionDescriptor" in one_entry["taskSpec"]
            assert type(one_entry["taskSpec"]["functionDescriptor"]) is dict
            assert "address" in one_entry
            assert type(one_entry["address"]) is dict
            assert "state" in one_entry
            assert "name" in one_entry
            assert "numRestarts" in one_entry
            assert "pid" in one_entry
            all_pids = {entry["pid"] for entry in actors.values()}
            assert 0 in all_pids  # The infeasible actor
            assert len(all_pids) > 1
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


def test_kill_actor(ray_start_with_dashboard):
    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self):
            ray.show_in_dashboard("test")
            return os.getpid()

    a = Actor.remote()
    worker_pid = ray.get(a.f.remote())  # noqa

    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    def actor_killed(pid):
        """Check For the existence of a unix pid."""
        try:
            os.kill(pid, 0)
        except OSError:
            return True
        else:
            return False

    def get_actor():
        resp = requests.get(f"{webui_url}/logical/actor_groups")
        resp.raise_for_status()
        actor_groups_resp = resp.json()
        assert actor_groups_resp["result"] is True, actor_groups_resp["msg"]
        actor_groups = actor_groups_resp["data"]["actorGroups"]
        actor = actor_groups["Actor"]["entries"][0]
        return actor

    def kill_actor_using_dashboard(actor):
        resp = requests.get(
            webui_url + "/logical/kill_actor",
            params={
                "actorId": actor["actorId"],
                "ipAddress": actor["ipAddress"],
                "port": actor["port"]
            })
        resp.raise_for_status()
        resp_json = resp.json()
        assert resp_json["result"] is True, "msg" in resp_json

    start = time.time()
    last_exc = None
    while time.time() - start <= 10:
        try:
            actor = get_actor()
            kill_actor_using_dashboard(actor)
            last_exc = None
            break
        except (KeyError, AssertionError) as e:
            last_exc = e
            time.sleep(.1)
    assert last_exc is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
