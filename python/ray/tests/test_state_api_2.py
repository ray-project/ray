import asyncio
import json
import os

from collections import defaultdict

import ray
import requests
import pytest

from ray._private.profiling import chrome_tracing_dump
from ray.experimental.state.api import list_tasks, list_actors, list_workers, list_nodes
from ray._private.test_utils import wait_for_condition


def test_timeline(shutdown_only):
    ray.init(num_cpus=8)
    job_id = ray.get_runtime_context().get_job_id()
    TASK_SLEEP_TIME_S = 1

    @ray.remote
    def f():
        import time

        time.sleep(TASK_SLEEP_TIME_S)

    @ray.remote
    class Actor:
        def ready(self):
            pass

    @ray.remote
    class AsyncActor:
        async def f(self):
            await asyncio.sleep(5)

        async def g(self):
            await asyncio.sleep(5)

    @ray.remote
    class ThreadedActor:
        def f(self):
            import time

            time.sleep(5)

        def g(self):
            import time

            time.sleep(5)

    [f.remote() for _ in range(4)]
    a = Actor.remote()
    b = AsyncActor.remote()
    c = ThreadedActor.options(max_concurrency=15).remote()

    [a.ready.remote() for _ in range(4)]
    ray.get(b.f.remote())
    [b.f.remote() for _ in range(4)]
    [b.g.remote() for _ in range(4)]
    [c.f.remote() for _ in range(4)]
    [c.g.remote() for _ in range(4)]

    result = json.loads(chrome_tracing_dump(list_tasks(detail=True)))

    # ph is the type of the event
    actor_to_events = defaultdict(list)
    task_to_events = defaultdict(list)
    index_to_workers = {}
    index_to_nodes = {}

    for item in result:
        if item["ph"] == "M":
            # metadata event
            name = item["name"]
            if name == "thread_name":
                index_to_workers[item["tid"]] = item["args"]["name"]
            elif name == "process_name":
                index_to_nodes[item["pid"]] = item["args"]["name"]
            else:
                raise ValueError(f"Unexecpted name from metadata event {name}")
        elif item["ph"] == "X":
            # regular interval event
            actor_id = item["args"]["actor_id"]
            assert "actor_id" in item["args"]
            assert "attempt_number" in item["args"]
            assert "func_or_class_name" in item["args"]
            assert "job_id" in item["args"]
            assert "task_id" in item["args"]

            if actor_id:
                actor_to_events[actor_id].append(item)
            else:
                task_to_events[item["args"]["task_id"]].append(item)
        else:
            raise ValueError(f"Unexpected event type {item['ph']}")

    actors = {actor["actor_id"]: actor for actor in list_actors(detail=True)}
    tasks = {task["task_id"]: task for task in list_tasks(detail=True)}
    workers = {worker["worker_id"]: worker for worker in list_workers(detail=True)}
    nodes = {node["node_ip"]: node for node in list_nodes(detail=True)}

    for actor_id, events in actor_to_events.items():
        # Event type is tested from test_advanced.py::test_profiling_api
        for event in events:
            # Make sure actor id is correctly set.
            assert event["args"]["actor_id"] == actor_id
            assert event["args"]["job_id"] == job_id
            task_id = event["args"]["task_id"]
            assert (
                event["args"]["func_or_class_name"]
                == tasks[task_id]["func_or_class_name"]
            )  # noqa
        # Make sure the worker id is correct.
        # ID is recorded as [worker_type]:[worker_id]
        worker_id_from_event = index_to_workers[event["tid"]].split(":")[1]
        # Node is recorded as Node [ip_address]
        node_id_from_event = index_to_nodes[event["pid"]].split(" ")[1]
        assert actors[actor_id]["pid"] == workers[worker_id_from_event]["pid"]
        assert actors[actor_id]["node_id"] == nodes[node_id_from_event]["node_id"]

    for task_id, events in task_to_events.items():
        for event in events:
            # Make sure actor id is correctly set.
            assert event["args"]["job_id"] == job_id
            task_id = event["args"]["task_id"]
            assert (
                event["args"]["func_or_class_name"]
                == tasks[task_id]["func_or_class_name"]
            )  # noqa
            # Make sure the duration is correct.
            # duration is in microseconds.
            # Since the task sleeps for TASK_SLEEP_TIME_S,
            # task:execute should have a similar sleep time.
            if event["cat"] == "task:execute":
                assert (
                    TASK_SLEEP_TIME_S * 1e6 * 0.9
                    < event["dur"]
                    < TASK_SLEEP_TIME_S * 1e6 * 1.1
                )  # noqa
        # Make sure the worker id is correct.
        worker_id_from_event = index_to_workers[event["tid"]].split(":")[1]
        node_id_from_event = index_to_nodes[event["pid"]].split(" ")[1]
        assert tasks[task_id]["worker_id"] == worker_id_from_event
        assert tasks[task_id]["node_id"] == nodes[node_id_from_event]["node_id"]

    # Verify the number of metadata events are correct.
    metadata_events = list(filter(lambda e: e["ph"] == "M", result))
    assert len(metadata_events) == len(index_to_workers) + len(index_to_nodes)


def test_timeline_request(shutdown_only):
    context = ray.init()
    dashboard_url = f"http://{context['webui_url']}"

    @ray.remote
    def f():
        pass

    ray.get([f.remote() for _ in range(5)])

    # Make sure the API works.
    def verify():
        resp = requests.get(f"{dashboard_url}/api/v0/tasks/timeline")
        resp.raise_for_status()
        assert resp.json(), "No result has returned"
        return True

    wait_for_condition(verify, timeout=10)


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
