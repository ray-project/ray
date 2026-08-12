import asyncio
import json
import os
import sys
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

import ray
import ray._private.state
from ray._common.test_utils import run_string_as_driver, wait_for_condition
from ray._common.utils import binary_to_hex
from ray._private.profiling import chrome_tracing_dump
from ray._private.test_utils import (
    check_call_subprocess,
    wait_for_aggregator_agent_if_enabled,
)
from ray.core.generated import gcs_pb2, gcs_service_pb2
from ray.util.state import (
    get_actor,
    list_actors,
    list_nodes,
    list_tasks,
    list_workers,
)

pytestmark = [
    pytest.mark.parametrize(
        "event_routing_config", ["default", "aggregator"], indirect=True
    ),
    pytest.mark.usefixtures("event_routing_config"),
]


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
    for name in list(sys.modules):
        if name.startswith("ray.experimental.state"):
            sys.modules.pop(name, None)

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
    wait_for_aggregator_agent_if_enabled(
        ray_start_with_dashboard["gcs_address"], ray_start_with_dashboard["node_id"]
    )

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
def test_state_api_scale_smoke(shutdown_only, monkeypatch):
    address_info = ray.init()
    wait_for_aggregator_agent_if_enabled(
        address_info["gcs_address"], address_info["node_id"]
    )
    monkeypatch.setenv("RAY_ADDRESS", address_info["gcs_address"])
    release_test_file_path = (
        "../../release/nightly_tests/stress_tests/test_state_api_scale.py"
    )
    full_path = Path(ray.__file__).parents[0] / release_test_file_path
    assert full_path.exists()

    check_call_subprocess(["python", str(full_path), "--smoke-test"])


def test_ray_timeline(shutdown_only):
    context = ray.init(num_cpus=8)
    wait_for_aggregator_agent_if_enabled(context["gcs_address"], context["node_id"])

    @ray.remote
    def f():
        import time

        time.sleep(0.1)

    ray.get(f.remote())

    with tempfile.TemporaryDirectory() as tmpdirname:
        filename = os.path.join(tmpdirname, "timeline.json")

        def verify():
            ray.timeline(filename)
            with open(filename, "r") as timeline_file:
                dumped = json.load(timeline_file)
            # TODO(swang): Check actual content. It doesn't seem to match the
            # return value of chrome_tracing_dump in above tests?
            assert len(dumped) > 0
            return True

        wait_for_condition(verify, timeout=20, retry_interval_ms=1000)


def _profile_task_event(component_type, component_id, event_name):
    task_event = gcs_pb2.TaskEvents()
    profile = task_event.profile_events
    profile.component_type = component_type
    profile.component_id = component_id
    profile.node_ip_address = "1.2.3.4"
    entry = profile.events.add()
    entry.event_name = event_name
    entry.start_time = 1
    entry.end_time = 2
    entry.extra_data = "{}"
    return task_event


def test_profile_events_reads_from_dashboard_head(monkeypatch):
    """With the migration flag on, profile_events() fetches task events from the dashboard
    head through a reused client that resolves the address once and reuses its session."""
    monkeypatch.setattr(
        "ray._private.state._READ_TASK_EVENTS_FROM_DASHBOARD_HEAD", True
    )

    component_id = b"\x01" * 28
    reply = gcs_service_pb2.GetTaskEventsReply()
    reply.status.SetInParent()
    reply.events_by_task.add().CopyFrom(
        _profile_task_event("worker", component_id, "task:execute")
    )

    gs = ray._private.state.GlobalState()
    accessor = MagicMock()
    accessor.get_internal_kv.return_value = b"127.0.0.1:8265"
    monkeypatch.setattr(gs, "_connect_and_get_accessor", lambda: accessor)
    # The client getter reads _global_state_accessor directly so set it too.
    gs._global_state_accessor = accessor

    session = MagicMock()
    session.post.return_value = MagicMock(
        status_code=200, content=reply.SerializeToString()
    )
    with patch("requests.Session", return_value=session):
        result = gs.profile_events()
        gs.profile_events()  # second call reuses the cached endpoint + session

    accessor.get_task_events.assert_not_called()
    # Address resolved once and the session reused across both timeline calls.
    assert accessor.get_internal_kv.call_count == 1
    assert session.post.call_count == 2
    assert session.post.call_args.args[0].endswith("/api/task_events/query")
    assert result[binary_to_hex(component_id)][0]["event_type"] == "task:execute"


def test_profile_events_reads_from_gcs_by_default(monkeypatch):
    """With the migration flag off, profile_events() reads from GCS via the accessor and
    never builds the dashboard-head client."""
    monkeypatch.setattr(
        "ray._private.state._READ_TASK_EVENTS_FROM_DASHBOARD_HEAD", False
    )

    component_id = b"\x02" * 28
    task_event = _profile_task_event("driver", component_id, "driver:startup")

    gs = ray._private.state.GlobalState()
    accessor = MagicMock()
    accessor.get_task_events.return_value = [task_event.SerializeToString()]
    monkeypatch.setattr(gs, "_connect_and_get_accessor", lambda: accessor)

    with patch("requests.Session") as mock_session_cls:
        result = gs.profile_events()

    accessor.get_task_events.assert_called_once()
    mock_session_cls.assert_not_called()
    assert result[binary_to_hex(component_id)][0]["event_type"] == "driver:startup"


def test_profile_events_gcs_and_head_paths_return_same_data(monkeypatch):
    """Migration parity: the GCS read path and the dashboard-head read path return the
    same profile_events() output for the same set of stored task events."""
    component_id = b"\x03" * 28
    events = [
        _profile_task_event("worker", component_id, "task:execute"),
        _profile_task_event("driver", component_id, "driver:startup"),
    ]

    # GCS path (flag off): the accessor returns serialized TaskEvents.
    monkeypatch.setattr(
        "ray._private.state._READ_TASK_EVENTS_FROM_DASHBOARD_HEAD", False
    )
    gs_gcs = ray._private.state.GlobalState()
    gcs_accessor = MagicMock()
    gcs_accessor.get_task_events.return_value = [e.SerializeToString() for e in events]
    monkeypatch.setattr(gs_gcs, "_connect_and_get_accessor", lambda: gcs_accessor)
    result_gcs = gs_gcs.profile_events()

    # Dashboard-head path (flag on): the head returns the same events in a reply.
    monkeypatch.setattr(
        "ray._private.state._READ_TASK_EVENTS_FROM_DASHBOARD_HEAD", True
    )
    reply = gcs_service_pb2.GetTaskEventsReply()
    reply.status.SetInParent()
    for e in events:
        reply.events_by_task.add().CopyFrom(e)
    gs_head = ray._private.state.GlobalState()
    head_accessor = MagicMock()
    head_accessor.get_internal_kv.return_value = b"127.0.0.1:8265"
    monkeypatch.setattr(gs_head, "_connect_and_get_accessor", lambda: head_accessor)
    gs_head._global_state_accessor = head_accessor
    session = MagicMock()
    session.post.return_value = MagicMock(
        status_code=200, content=reply.SerializeToString()
    )
    with patch("requests.Session", return_value=session):
        result_head = gs_head.profile_events()

    # Both read paths surface identical profiling data.
    assert result_gcs == result_head
    assert result_gcs[binary_to_hex(component_id)][0]["event_type"] == "task:execute"


# Tasks the parity driver runs; each one produces exactly one task:execute profile event,
# so this is also the task:execute count each routing path must report.
_TIMELINE_PARITY_NUM_TASKS = 5

# Driver for the GCS-vs-head timeline parity e2e. Run once per backend via
# run_string_as_driver
_TIMELINE_PARITY_DRIVER = f"""
import ray
from ray._common.test_utils import wait_for_condition

NUM_TASKS = {_TIMELINE_PARITY_NUM_TASKS}


@ray.remote
def f():
    import time

    time.sleep(0.2)


ray.init(num_cpus=4)
ray.get([f.remote() for _ in range(NUM_TASKS)])


def task_execute_count():
    return sum(1 for event in ray.timeline() if event.get("cat") == "task:execute")


# Profile events propagate asynchronously; wait until every task-execute span lands.
wait_for_condition(
    lambda: task_execute_count() >= NUM_TASKS, timeout=90, retry_interval_ms=3000
)
print("TASK_EXECUTE_COUNT:" + str(task_execute_count()))
"""

# Task-event routing flags this test sets explicitly; stripped from the inherited env so
# the event_routing_config fixture's settings don't leak into the driver subprocesses.
# Matched upper-cased: Windows upper-cases every os.environ key, so we don't want to
# do a case-sensitive comparison.
_TASK_EVENT_FLAG_ENV = frozenset(
    name.upper()
    for name in (
        "RAY_enable_ray_event",
        "RAY_enable_ray_task_event_recorder",
        "RAY_enable_task_events_to_dashboard_head",
        "RAY_enable_core_worker_task_event_to_gcs",
        "RAY_enable_core_worker_ray_event_to_aggregator",
        "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_GCS",
    )
)


def _run_timeline_parity_driver(flag_overrides):
    env = {k: v for k, v in os.environ.items() if k.upper() not in _TASK_EVENT_FLAG_ENV}
    # ray.timeline() only records profile events when profiling is on (see
    # GlobalState.chrome_tracing_dump).
    env["RAY_PROFILING"] = "1"
    env.update(flag_overrides)
    out = run_string_as_driver(_TIMELINE_PARITY_DRIVER, env=env)
    marker = "TASK_EXECUTE_COUNT:"
    matches = [line for line in out.splitlines() if line.startswith(marker)]
    assert matches, f"driver did not report a count; output:\n{out}"
    return int(matches[-1][len(marker) :])


def test_timeline_gcs_and_head_paths_produce_same_events():
    """End-to-end parity: the same workload run through the GCS path and through the
    dashboard-head path (recorder -> aggregator -> head) each surface one
    ray.timeline() task-execute event per task. Two driver subprocesses are used because
    the read/publish switches are baked at `import ray` and can't be flipped in-process.
    """
    gcs_count = _run_timeline_parity_driver(
        {
            "RAY_enable_core_worker_task_event_to_gcs": "1",
            "RAY_enable_core_worker_ray_event_to_aggregator": "0",
            "RAY_enable_ray_event": "0",
            "RAY_enable_ray_task_event_recorder": "0",
            "RAY_enable_task_events_to_dashboard_head": "0",
        }
    )
    head_count = _run_timeline_parity_driver(
        {
            "RAY_enable_ray_event": "1",
            "RAY_enable_ray_task_event_recorder": "1",
            "RAY_enable_task_events_to_dashboard_head": "1",
            "RAY_enable_core_worker_task_event_to_gcs": "0",
            "RAY_enable_core_worker_ray_event_to_aggregator": "0",
        }
    )

    assert gcs_count == _TIMELINE_PARITY_NUM_TASKS, (
        "GCS-path timeline task:execute events: "
        f"{gcs_count}, expected {_TIMELINE_PARITY_NUM_TASKS}"
    )
    assert head_count == _TIMELINE_PARITY_NUM_TASKS, (
        "dashboard-head-path timeline task:execute events: "
        f"{head_count}, expected {_TIMELINE_PARITY_NUM_TASKS}"
    )


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
