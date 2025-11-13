import asyncio
import json
import os
import sys
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import requests

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.profiling import chrome_tracing_dump
from ray._private.test_utils import check_call_subprocess
from ray.util.state import (
    get_actor,
    list_actors,
    list_nodes,
    list_tasks,
    list_workers,
)


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


def test_actor_repr_name(shutdown_only):
    def _verify_repr_name(id, name):
        actor = get_actor(id=id)
        assert actor is not None
        assert actor["repr_name"] == name
        return True

    # Assert simple actor repr name
    @ray.remote
    class ReprActor:
        def __init__(self, x) -> None:
            self.x = x

        def __repr__(self) -> str:
            return self.x

        def ready(self):
            pass

    a = ReprActor.remote(x="repr-name-a")
    b = ReprActor.remote(x="repr-name-b")

    wait_for_condition(_verify_repr_name, id=a._actor_id.hex(), name="repr-name-a")
    wait_for_condition(_verify_repr_name, id=b._actor_id.hex(), name="repr-name-b")

    # Assert when no __repr__ defined. repr_name should be empty
    @ray.remote
    class Actor:
        pass

    a = Actor.remote()
    wait_for_condition(_verify_repr_name, id=a._actor_id.hex(), name="")

    # Assert special actors (async actor, threaded actor, detached actor, named actor)
    @ray.remote
    class AsyncActor:
        def __init__(self, x) -> None:
            self.x = x

        def __repr__(self) -> str:
            return self.x

        async def ready(self):
            pass

    a = AsyncActor.remote(x="async-x")
    wait_for_condition(_verify_repr_name, id=a._actor_id.hex(), name="async-x")

    a = ReprActor.options(max_concurrency=3).remote(x="x")
    wait_for_condition(_verify_repr_name, id=a._actor_id.hex(), name="x")

    a = ReprActor.options(name="named-actor").remote(x="repr-name")
    wait_for_condition(_verify_repr_name, id=a._actor_id.hex(), name="repr-name")

    a = ReprActor.options(name="detached-actor", lifetime="detached").remote(
        x="repr-name"
    )
    wait_for_condition(_verify_repr_name, id=a._actor_id.hex(), name="repr-name")
    ray.kill(a)

    # Assert nested actor class.
    class OutClass:
        @ray.remote
        class InnerActor:
            def __init__(self, name) -> None:
                self.name = name

            def __repr__(self) -> str:
                return self.name

        def get_actor(self, name):
            return OutClass.InnerActor.remote(name=name)

    a = OutClass().get_actor(name="inner")
    wait_for_condition(_verify_repr_name, id=a._actor_id.hex(), name="inner")


def test_experimental_import_deprecation():
    with pytest.warns(DeprecationWarning):
        from ray.experimental.state.api import list_tasks  # noqa: F401

    with pytest.warns(DeprecationWarning):
        from ray.experimental.state.common import DEFAULT_RPC_TIMEOUT  # noqa: F401

    with pytest.warns(DeprecationWarning):
        from ray.experimental.state.custom_types import ACTOR_STATUS  # noqa: F401

    with pytest.warns(DeprecationWarning):
        from ray.experimental.state.exception import RayStateApiException  # noqa: F401

    with pytest.warns(DeprecationWarning):
        from ray.experimental.state.state_cli import ray_get  # noqa: F401

    with pytest.warns(DeprecationWarning):
        from ray.experimental.state.state_manager import (  # noqa: F401
            StateDataSourceClient,
        )

    with pytest.warns(DeprecationWarning):
        from ray.experimental.state.util import convert_string_to_type  # noqa: F401


def test_actor_task_with_repr_name(ray_start_with_dashboard):
    @ray.remote
    class ReprActor:
        def __init__(self, x) -> None:
            self.x = x

        def __repr__(self) -> str:
            return self.x

        def f(self):
            pass

    a = ReprActor.remote(x="repr-name-a")
    ray.get(a.f.remote())

    def verify():
        tasks = list_tasks(detail=True, filters=[("type", "=", "ACTOR_TASK")])
        assert len(tasks) == 1, tasks
        assert tasks[0].name == "repr-name-a.f"
        assert tasks[0].func_or_class_name == "ReprActor.f"
        return True

    wait_for_condition(verify)

    b = ReprActor.remote(x="repr-name-b")
    ray.get(b.f.options(name="custom-name").remote())

    def verify():
        tasks = list_tasks(
            detail=True,
            filters=[("actor_id", "=", b._actor_id.hex()), ("type", "=", "ACTOR_TASK")],
        )
        assert len(tasks) == 1, tasks
        assert tasks[0].name == "custom-name"
        assert tasks[0].func_or_class_name == "ReprActor.f"
        return True

    wait_for_condition(verify)

    @ray.remote
    class Actor:
        def f(self):
            pass

    c = Actor.remote()
    ray.get(c.f.remote())

    def verify():
        tasks = list_tasks(
            detail=True,
            filters=[("actor_id", "=", c._actor_id.hex()), ("type", "=", "ACTOR_TASK")],
        )

        assert len(tasks) == 1, tasks
        assert tasks[0].name == "Actor.f"
        assert tasks[0].func_or_class_name == "Actor.f"
        return True

    wait_for_condition(verify)


@pytest.mark.skipif(
    sys.platform == "win32", reason="Release test not expected to work on non-linux."
)
def test_state_api_scale_smoke(shutdown_only):
    ray.init()
    release_test_file_path = (
        "../../release/nightly_tests/stress_tests/test_state_api_scale.py"
    )
    full_path = Path(ray.__file__).parents[0] / release_test_file_path
    assert full_path.exists()

    check_call_subprocess(["python", str(full_path), "--smoke-test"])


def test_ray_timeline(shutdown_only):
    ray.init(num_cpus=8)

    @ray.remote
    def f():
        pass

    ray.get(f.remote())

    with tempfile.TemporaryDirectory() as tmpdirname:
        filename = os.path.join(tmpdirname, "timeline.json")
        ray.timeline(filename)

        with open(filename, "r") as f:
            dumped = json.load(f)
        # TODO(swang): Check actual content. It doesn't seem to match the
        # return value of chrome_tracing_dump in above tests?
        assert len(dumped) > 0


def test_state_init_multiple_threads(shutdown_only):
    ray.init()
    global_state = ray._private.state.state
    global_state._connect_and_get_accessor()
    gcs_options = global_state.gcs_options

    def disconnect():
        global_state.disconnect()
        global_state._initialize_global_state(gcs_options)
        return True

    def get_nodes_from_state_api():
        try:
            return len(global_state.node_table()) == 1
        except ray.exceptions.RaySystemError:
            # There's a gap between disconnect and _initialize_global_state
            # and this will be raised if we try to connect during that gap
            return True

    disconnect()
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(get_nodes_from_state_api) for _ in range(50)]
        futures.extend([executor.submit(disconnect) for _ in range(50)])
        futures.extend([executor.submit(get_nodes_from_state_api) for _ in range(50)])
        results = [future.result() for future in futures]

    # Assert that all calls returned True
    assert all(results)
    assert len(results) == 150


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
