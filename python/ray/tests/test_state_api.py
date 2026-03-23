import json
import os
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional
from unittest.mock import AsyncMock

import pytest
import yaml
from click.testing import CliRunner

import ray
import ray._private.ray_constants as ray_constants
import ray._private.state as global_state
from ray._common.network_utils import find_free_port, parse_address
from ray._common.test_utils import (
    SignalActor,
    async_wait_for_condition,
    wait_for_condition,
)
from ray._private.grpc_utils import init_grpc_channel
from ray._private.state_api_test_utils import create_api_options
from ray._raylet import GcsClient, NodeID
from ray.cluster_utils import cluster_not_supported
from ray.core.generated.common_pb2 import (
    Address,
    CoreWorkerStats,
    ObjectRefInfo,
    TaskInfoEntry,
    TaskStatus,
    TaskType,
    WorkerType,
)
from ray.core.generated.gcs_pb2 import (
    ActorTableData,
    TaskEvents,
    TaskStateUpdate,
)
from ray.core.generated.gcs_service_pb2 import (
    GcsStatus,
    GetAllActorInfoReply,
    GetTaskEventsReply,
)
from ray.dashboard.state_aggregator import StateAPIManager
from ray.dashboard.state_api_utils import convert_filters_type
from ray.dashboard.utils import ray_address_to_api_server_url
from ray.job_submission import JobSubmissionClient
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.util.state import (
    StateApiClient,
    get_actor,
    list_actors,
    list_cluster_events,
    list_jobs,
    list_nodes,
    list_objects,
    list_placement_groups,
    list_runtime_envs,
    list_tasks,
    list_workers,
    summarize_actors,
    summarize_objects,
    summarize_tasks,
)
from ray.util.state.common import (
    ActorState,
    Humanify,
    ObjectState,
    RuntimeEnvState,
    StateSchema,
    state_column,
)
from ray.util.state.exception import DataSourceUnavailable, RayStateApiException
from ray.util.state.state_cli import (
    AvailableFormat,
    _parse_filter,
    format_list_api_output,
    ray_get,
    ray_list,
    summary_state_cli_group,
)
from ray.util.state.state_manager import StateDataSourceClient

"""
Unit tests
"""


@pytest.fixture
def state_api_manager():
    data_source_client = AsyncMock(StateDataSourceClient)
    manager = StateAPIManager(
        data_source_client, thread_pool_executor=ThreadPoolExecutor()
    )
    yield manager


def state_source_client(gcs_address):
    GRPC_CHANNEL_OPTIONS = (
        *ray_constants.GLOBAL_GRPC_OPTIONS,
        ("grpc.max_send_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
        ("grpc.max_receive_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
    )
    gcs_channel = init_grpc_channel(
        gcs_address, GRPC_CHANNEL_OPTIONS, asynchronous=True
    )
    gcs_client = GcsClient(address=gcs_address)
    client = StateDataSourceClient(gcs_channel=gcs_channel, gcs_client=gcs_client)
    return client


def generate_actor_data(id, state=ActorTableData.ActorState.ALIVE, class_name="class"):
    return ActorTableData(
        actor_id=id,
        state=state,
        name="abc",
        pid=1234,
        class_name=class_name,
        address=Address(node_id=id, ip_address="127.0.0.1", port=124, worker_id=id),
        job_id=b"123",
        node_id=None,
        ray_namespace="",
    )


def generate_task_event(
    id,
    name="class",
    func_or_class="class",
    state=TaskStatus.PENDING_NODE_ASSIGNMENT,
    type=TaskType.NORMAL_TASK,
    node_id=NodeID.from_random(),
    attempt_number=0,
    job_id=b"0001",
):
    if node_id is not None:
        node_id = node_id.binary()

    task_info = TaskInfoEntry(
        task_id=id,
        name=name,
        func_or_class_name=func_or_class,
        type=type,
    )
    state_updates = TaskStateUpdate(
        node_id=node_id,
        state_ts_ns={state: 1},
    )
    return TaskEvents(
        task_id=id,
        job_id=job_id,
        attempt_number=attempt_number,
        task_info=task_info,
        state_updates=state_updates,
    )


def generate_task_data(events_by_task):
    return GetTaskEventsReply(
        status=GcsStatus(),
        events_by_task=events_by_task,
        num_status_task_events_dropped=0,
        num_profile_task_events_dropped=0,
        num_total_stored=len(events_by_task),
    )


def generate_object_info(
    obj_id,
    size_bytes=1,
    callsite="main.py",
    task_state=TaskStatus.PENDING_NODE_ASSIGNMENT,
    local_ref_count=1,
    attempt_number=1,
    pid=1234,
    ip="1234",
    worker_type=WorkerType.DRIVER,
    pinned_in_memory=True,
):
    return CoreWorkerStats(
        pid=pid,
        worker_type=worker_type,
        ip_address=ip,
        object_refs=[
            ObjectRefInfo(
                object_id=obj_id,
                call_site=callsite,
                object_size=size_bytes,
                local_ref_count=local_ref_count,
                submitted_task_ref_count=1,
                contained_in_owned=[],
                pinned_in_memory=pinned_in_memory,
                task_status=task_state,
                attempt_number=attempt_number,
            )
        ],
    )


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_ray_address_to_api_server_url(shutdown_only):
    ctx = ray.init()
    api_server_url = f'http://{ctx.address_info["webui_url"]}'
    address = ctx.address_info["address"]
    gcs_address = ctx.address_info["gcs_address"]

    # None should auto detect current ray address
    assert api_server_url == ray_address_to_api_server_url(None)
    # 'auto' should get
    assert api_server_url == ray_address_to_api_server_url("auto")
    # ray address
    assert api_server_url == ray_address_to_api_server_url(address)
    # explicit head node gcs address
    assert api_server_url == ray_address_to_api_server_url(gcs_address)
    # localhost string
    _, gcs_port = parse_address(gcs_address)
    assert api_server_url == ray_address_to_api_server_url(f"localhost:{gcs_port}")


def test_state_schema():
    import pydantic
    from pydantic.dataclasses import dataclass

    @dataclass
    class TestSchema(StateSchema):
        column_a: int
        column_b: int = state_column(filterable=False)
        column_c: int = state_column(filterable=True)
        column_d: int = state_column(filterable=False, detail=False)
        column_f: int = state_column(filterable=True, detail=False)
        column_e: int = state_column(filterable=False, detail=True)
        column_g: int = state_column(filterable=True, detail=True)

    # Correct input validation should work without an exception.
    TestSchema(
        column_a=1,
        column_b=1,
        column_c=1,
        column_d=1,
        column_e=1,
        column_f=1,
        column_g=1,
    )

    # Incorrect input type.
    with pytest.raises(pydantic.ValidationError):
        TestSchema(
            column_a=1,
            column_b=1,
            column_c=1,
            column_d=1,
            column_e=1,
            column_f=1,
            column_g="a",
        )

    assert TestSchema.filterable_columns() == {
        "column_c",
        "column_f",
        "column_g",
    }

    assert TestSchema.base_columns() == {
        "column_a",
        "column_b",
        "column_c",
        "column_d",
        "column_f",
    }

    assert TestSchema.columns() == {
        "column_a",
        "column_b",
        "column_c",
        "column_d",
        "column_e",
        "column_f",
        "column_g",
    }


def test_parse_filter():
    # Basic
    assert _parse_filter("key=value") == ("key", "=", "value")
    assert _parse_filter("key!=value") == ("key", "!=", "value")

    # Predicate =
    assert _parse_filter("key=value=123=1") == ("key", "=", "value=123=1")
    assert _parse_filter("key=value!=123!=1") == ("key", "=", "value!=123!=1")
    assert _parse_filter("key=value!=123=1") == ("key", "=", "value!=123=1")
    assert _parse_filter("key=value!=123=1!") == ("key", "=", "value!=123=1!")
    assert _parse_filter("key=value!=123=1=") == ("key", "=", "value!=123=1=")
    assert _parse_filter("key=value!=123=1!=") == ("key", "=", "value!=123=1!=")

    # Predicate !=
    assert _parse_filter("key!=value=123=1") == ("key", "!=", "value=123=1")
    assert _parse_filter("key!=value!=123!=1") == ("key", "!=", "value!=123!=1")
    assert _parse_filter("key!=value!=123=1") == ("key", "!=", "value!=123=1")
    assert _parse_filter("key!=value!=123=1!") == ("key", "!=", "value!=123=1!")
    assert _parse_filter("key!=value!=123=1=") == ("key", "!=", "value!=123=1=")
    assert _parse_filter("key!=value!=123=1!=") == ("key", "!=", "value!=123=1!=")

    # Incorrect cases
    with pytest.raises(ValueError):
        _parse_filter("keyvalue")

    with pytest.raises(ValueError):
        _parse_filter("keyvalue!")
    with pytest.raises(ValueError):
        _parse_filter("keyvalue!=")
    with pytest.raises(ValueError):
        _parse_filter("keyvalue=")

    with pytest.raises(ValueError):
        _parse_filter("!keyvalue")
    with pytest.raises(ValueError):
        _parse_filter("!=keyvalue")
    with pytest.raises(ValueError):
        _parse_filter("=keyvalue")

    with pytest.raises(ValueError):
        _parse_filter("=keyvalue=")
    with pytest.raises(ValueError):
        _parse_filter("!=keyvalue=")
    with pytest.raises(ValueError):
        _parse_filter("=keyvalue!=")
    with pytest.raises(ValueError):
        _parse_filter("!=keyvalue!=")

    with pytest.raises(ValueError):
        _parse_filter("key>value")
    with pytest.raises(ValueError):
        _parse_filter("key>value!=")


# Without this, capsys will have a race condition
# that causes
# ValueError: I/O operation on closed file.
@pytest.fixture
def clear_loggers():
    """Remove handlers from all loggers"""
    yield
    import logging

    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_state_api_client_periodic_warning(shutdown_only, capsys, clear_loggers):
    ray.init()
    timeout = 10
    StateApiClient()._make_http_get_request("/api/v0/delay/5", {}, timeout, True)
    captured = capsys.readouterr()
    lines = captured.err.strip().split("\n")
    # Lines are printed 1.25, 2.5, and 5 seconds.
    # First line is the dashboard start log.
    # INFO services.py:1477 -- View the Ray dashboard at http://127.0.0.1:8265
    print(lines)

    expected_elapsed = [1.25, 2.5, 5.0]
    expected_lines = []
    for elapsed in expected_elapsed:
        expected_lines.append(
            f"({elapsed} / 10 seconds) Waiting for the "
            "response from the API "
            "server address http://127.0.0.1:8265/api/v0/delay/5."
        )
    for expected_line in expected_lines:
        expected_line in lines


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exception", "status_code"),
    [
        (None, 200),
        (ValueError("Invalid filter parameter"), 400),
        (DataSourceUnavailable("GCS connection failed"), 500),
    ],
)
async def test_handle_list_api_status_codes(
    exception: Optional[Exception], status_code: int
):
    """Test that handle_list_api calls do_reply with correct status codes.

    This directly tests the HTTP layer logic that maps exceptions to status codes:
    - Success → HTTP 200 OK
    - ValueError → HTTP 400 BAD_REQUEST
    - DataSourceUnavailable → HTTP 500 INTERNAL_ERROR
    """
    from unittest.mock import AsyncMock, MagicMock

    from ray.dashboard.state_api_utils import handle_list_api
    from ray.util.state.common import ListApiResponse

    # 1. Mock aiohttp request with proper query interface
    mock_request = MagicMock()

    def mock_get(key, default=None):
        return default

    mock_request.query = MagicMock()
    mock_request.query.get = mock_get

    # 2. Mock response whether success or failure.
    if exception is None:
        mock_backend = AsyncMock(
            return_value=ListApiResponse(
                result=[],
                total=0,
                num_after_truncation=0,
                num_filtered=0,
                partial_failure_warning="",
            )
        )
    else:
        mock_backend = AsyncMock(side_effect=exception)

    response = await handle_list_api(mock_backend, mock_request)

    # 3. Assert status_code is correct.
    assert response.status == status_code


def test_type_conversion():
    # Test string
    r = convert_filters_type([("actor_id", "=", "123")], ActorState)
    assert r[0][2] == "123"
    r = convert_filters_type([("actor_id", "=", "abcd")], ActorState)
    assert r[0][2] == "abcd"
    r = convert_filters_type([("actor_id", "=", "True")], ActorState)
    assert r[0][2] == "True"

    # Test boolean
    r = convert_filters_type([("success", "=", "1")], RuntimeEnvState)
    assert r[0][2]
    r = convert_filters_type([("success", "=", "True")], RuntimeEnvState)
    assert r[0][2]
    r = convert_filters_type([("success", "=", "true")], RuntimeEnvState)
    assert r[0][2]
    with pytest.raises(ValueError):
        r = convert_filters_type([("success", "=", "random_string")], RuntimeEnvState)
    r = convert_filters_type([("success", "=", "false")], RuntimeEnvState)
    assert r[0][2] is False
    r = convert_filters_type([("success", "=", "False")], RuntimeEnvState)
    assert r[0][2] is False
    r = convert_filters_type([("success", "=", "0")], RuntimeEnvState)
    assert r[0][2] is False

    # Test int
    r = convert_filters_type([("pid", "=", "0")], ObjectState)
    assert r[0][2] == 0
    r = convert_filters_type([("pid", "=", "123")], ObjectState)
    assert r[0][2] == 123
    # Only integer can be provided.
    with pytest.raises(ValueError):
        r = convert_filters_type([("pid", "=", "123.3")], ObjectState)
    with pytest.raises(ValueError):
        r = convert_filters_type([("pid", "=", "abc")], ObjectState)

    # currently, there's no schema that has float column.


def test_humanify():
    raw_bytes = 1024
    assert Humanify.memory(raw_bytes) == "1.000 KiB"
    raw_bytes *= 1024
    assert Humanify.memory(raw_bytes) == "1.000 MiB"
    raw_bytes *= 1024
    assert Humanify.memory(raw_bytes) == "1.000 GiB"
    timestamp = 1610000000
    assert "1970-01" in Humanify.timestamp(timestamp)
    assert Humanify.duration(timestamp) == "18 days, 15:13:20"


def is_hex(val):
    try:
        int_val = int(val, 16)
    except ValueError:
        return False
    # Should remove leading 0 because when the value is converted back
    # to hex, it is removed.
    val = val.lstrip("0")
    return f"0x{val}" == hex(int_val)


"""
Integration tests
"""


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported on Windows")
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_cli_apis_sanity_check(ray_start_cluster):
    """Test all of CLI APIs work as expected."""
    NUM_NODES = 4
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    for _ in range(NUM_NODES - 1):
        cluster.add_node(num_cpus=2, dashboard_agent_listen_port=find_free_port())
    runner = CliRunner()

    client = JobSubmissionClient(
        f"http://{ray._private.worker.global_worker.node.address_info['webui_url']}"
    )

    @ray.remote
    def f():
        import time

        time.sleep(30)

    @ray.remote
    class Actor:
        pass

    obj = ray.put(3)  # noqa
    task = f.remote()  # noqa
    actor = Actor.remote()  # noqa
    actor_runtime_env = Actor.options(  # noqa
        runtime_env={"pip": ["requests"]}
    ).remote()
    job_id = client.submit_job(  # noqa
        # Entrypoint shell command to execute
        entrypoint="ls",
    )
    pg = ray.util.placement_group(bundles=[{"CPU": 1}])  # noqa

    def verify_output(cmd, args: List[str], necessary_substrings: List[str]):
        result = runner.invoke(cmd, args)
        print(result)
        exit_code_correct = result.exit_code == 0
        substring_matched = all(
            substr in result.output for substr in necessary_substrings
        )
        print(result.output)
        return exit_code_correct and substring_matched

    wait_for_condition(
        lambda: verify_output(ray_list, ["actors"], ["Stats:", "Table:", "ACTOR_ID"])
    )
    # TODO(sang): Enable it.
    # wait_for_condition(
    #     lambda: verify_output(
    #         ray_list, ["cluster-events"], ["Stats:", "Table:", "EVENT_ID"]
    #     )
    # )
    wait_for_condition(
        lambda: verify_output(ray_list, ["workers"], ["Stats:", "Table:", "WORKER_ID"])
    )
    wait_for_condition(
        lambda: verify_output(ray_list, ["nodes"], ["Stats:", "Table:", "NODE_ID"])
    )
    wait_for_condition(
        lambda: verify_output(
            ray_list, ["placement-groups"], ["Stats:", "Table:", "PLACEMENT_GROUP_ID"]
        )
    )
    wait_for_condition(lambda: verify_output(ray_list, ["jobs"], ["raysubmit"]))
    wait_for_condition(
        lambda: verify_output(ray_list, ["tasks"], ["Stats:", "Table:", "TASK_ID"])
    )
    wait_for_condition(
        lambda: verify_output(ray_list, ["objects"], ["Stats:", "Table:", "OBJECT_ID"])
    )
    wait_for_condition(
        lambda: verify_output(
            ray_list, ["runtime-envs"], ["Stats:", "Table:", "RUNTIME_ENV"]
        )
    )

    # Test get node by id
    nodes = ray.nodes()
    wait_for_condition(
        lambda: verify_output(
            ray_get, ["nodes", nodes[0]["NodeID"]], ["node_id", nodes[0]["NodeID"]]
        )
    )
    # Test get workers by id
    workers = global_state.workers()
    assert len(workers) > 0
    worker_id = list(workers.keys())[0]
    wait_for_condition(
        lambda: verify_output(ray_get, ["workers", worker_id], ["worker_id", worker_id])
    )

    # Test get actors by id
    wait_for_condition(
        lambda: verify_output(
            ray_get,
            ["actors", actor._actor_id.hex()],
            ["actor_id", actor._actor_id.hex()],
        )
    )

    # Test get task by ID
    wait_for_condition(
        lambda: verify_output(
            ray_get, ["tasks", task.task_id().hex()], ["task_id", task.task_id().hex()]
        )
    )

    # Test get placement groups by id
    wait_for_condition(
        lambda: verify_output(
            ray_get,
            ["placement-groups", pg.id.hex()],
            ["placement_group_id", pg.id.hex()],
        )
    )

    # Test get objects by id
    wait_for_condition(
        lambda: verify_output(ray_get, ["objects", obj.hex()], ["object_id", obj.hex()])
    )

    # Test address flag auto detection
    wait_for_condition(
        lambda: verify_output(
            ray_get,
            ["objects", obj.hex(), "--address", "auto"],
            ["object_id", obj.hex()],
        )
    )
    wait_for_condition(
        lambda: verify_output(
            ray_list, ["tasks", "--address", "auto"], ["Stats:", "Table:", "TASK_ID"]
        )
    )

    # TODO(rickyyx:alpha-obs):
    # - get job by id: jobs is not currently filterable by id
    # - get task by id: no easy access to tasks yet


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
@pytest.mark.parametrize(
    "override_url",
    [
        "https://external_dashboard_url",
        "https://external_dashboard_url/path1/?query_param1=val1&query_param2=val2",
        "new_external_dashboard_url",
    ],
)
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_state_api_with_external_dashboard_override(
    shutdown_only, override_url, monkeypatch
):
    with monkeypatch.context() as m:
        if override_url:
            m.setenv(
                ray_constants.RAY_OVERRIDE_DASHBOARD_URL,
                override_url,
            )

        ray.init()

        @ray.remote
        class A:
            pass

        a = A.remote()  # noqa

        def verify():
            # Test list
            actors = list_actors()
            assert len(actors) == 1
            assert actors[0]["state"] == "ALIVE"
            assert is_hex(actors[0]["actor_id"])
            assert a._actor_id.hex() == actors[0]["actor_id"]

            # Test get
            actors = list_actors(detail=True)
            for actor in actors:
                get_actor_data = get_actor(actor["actor_id"])
                assert get_actor_data is not None
                assert get_actor_data == actor

            return True

        wait_for_condition(verify)
        print(list_actors())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
async def test_cloud_envs(ray_start_cluster, monkeypatch):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, node_name="head_node")
    ray.init(address=cluster.address)
    with monkeypatch.context() as m:
        m.setenv(
            "RAY_CLOUD_INSTANCE_ID",
            "test_cloud_id",
        )
        m.setenv("RAY_NODE_TYPE_NAME", "test-node-type")
        cluster.add_node(
            num_cpus=1,
            node_name="worker_node",
            dashboard_agent_listen_port=find_free_port(),
        )
    client = state_source_client(cluster.address)

    async def verify():
        node_infos, _ = await client.get_all_node_info()
        assert len(node_infos) == 2
        for node_info in node_infos.values():
            if node_info.node_name == "worker_node":
                assert node_info.instance_id == "test_cloud_id"
                assert node_info.node_type_name == "test-node-type"
            else:
                assert node_info.instance_id == ""
                assert node_info.node_type_name == ""

        return True

    await async_wait_for_condition(verify)


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_pg_worker_id_tasks(shutdown_only):
    ray.init(num_cpus=1)
    pg = ray.util.placement_group(bundles=[{"CPU": 1}])
    pg.wait()

    @ray.remote
    def f():
        pass

    @ray.remote
    class A:
        def ready(self):
            return os.getpid()

    ray.get(
        f.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
    )

    def verify():
        tasks = list_tasks(detail=True)
        workers = list_workers(
            filters=[("worker_type", "=", "WORKER")], raise_on_missing_output=False
        )
        assert len(tasks) == 1
        assert len(workers) == 1

        assert tasks[0]["placement_group_id"] == pg.id.hex()
        assert tasks[0]["worker_id"] == workers[0]["worker_id"]
        assert tasks[0]["worker_pid"] == workers[0]["pid"]

        return True

    wait_for_condition(verify)
    print(list_tasks(detail=True))

    a = A.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
    ).remote()
    pid = ray.get(a.ready.remote())

    def verify():
        actors = list_actors(detail=True)
        workers = list_workers(
            detail=True, filters=[("pid", "=", pid)], raise_on_missing_output=False
        )
        assert len(actors) == 1
        assert len(workers) == 1

        assert actors[0]["placement_group_id"] == pg.id.hex()
        return True

    wait_for_condition(verify)
    print(list_actors(detail=True))


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_parent_task_id(shutdown_only):
    """Test parent task id set up properly"""
    ray.init(num_cpus=2)

    @ray.remote
    def child():
        pass

    @ray.remote
    def parent():
        ray.get(child.remote())

    ray.get(parent.remote())

    def verify():
        tasks = list_tasks(detail=True)
        assert len(tasks) == 2, "Expect 2 tasks to finished"
        parent_task_id = None
        child_parent_task_id = None
        for task in tasks:
            if task["func_or_class_name"] == "parent":
                parent_task_id = task["task_id"]
            elif task["func_or_class_name"] == "child":
                child_parent_task_id = task["parent_task_id"]

        assert (
            parent_task_id == child_parent_task_id
        ), "Child should have the parent task id"
        return True

    wait_for_condition(verify)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_network_failure(shutdown_only):
    """When the request fails due to network failure,
    verifies it raises an exception."""
    ray.init()

    @ray.remote
    def f():
        import time

        time.sleep(30)

    a = [f.remote() for _ in range(4)]  # noqa
    wait_for_condition(lambda: len(list_tasks()) == 4)

    # Kill raylet will not make list_tasks raise exceptions.
    ray._private.worker._global_node.kill_raylet()
    assert len(list_tasks()) == 4

    # Kill GCS so that list_tasks will have network error on querying tasks.
    ray._private.worker._global_node.kill_gcs_server()

    with pytest.raises(ray.exceptions.RpcError):
        list_tasks(_explain=True)


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_network_partial_failures(monkeypatch, ray_start_cluster):
    """When the request fails due to network failure,
    verifies it prints proper warning."""
    with monkeypatch.context() as m:
        # defer for 5s for the second node.
        # This will help the API not return until the node is killed.
        m.setenv(
            "RAY_testing_asio_delay_us",
            "NodeManagerService.grpc_server.GetObjectsInfo=5000000:5000000",
        )
        m.setenv("RAY_record_ref_creation_sites", "1")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=2)
        ray.init(address=cluster.address)
        n = cluster.add_node(num_cpus=2)

        @ray.remote
        def f():
            ray.put(1)

        a = [f.remote() for _ in range(4)]  # noqa
        wait_for_condition(lambda: len(list_objects()) == 4)

        # Make sure when there's 0 node failure, it doesn't print the error.
        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            list_objects(_explain=True)
        assert len(record) == 0

        # Kill raylet so that list_objects will have network error on querying raylets.
        cluster.remove_node(n, allow_graceful=False)

        with pytest.warns(UserWarning):
            list_objects(raise_on_missing_output=False, _explain=True)

        # Make sure when _explain == False, warning is not printed.
        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            list_objects(raise_on_missing_output=False, _explain=False)
        assert len(record) == 0


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_network_partial_failures_timeout(monkeypatch, ray_start_cluster):
    """When the request fails due to network timeout,
    verifies it prints proper warning."""
    monkeypatch.setenv("RAY_record_ref_creation_sites", "1")
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    with monkeypatch.context() as m:
        # defer for 10s for the second node.
        m.setenv(
            "RAY_testing_asio_delay_us",
            "NodeManagerService.grpc_server.GetObjectsInfo=10000000:10000000",
        )
        cluster.add_node(num_cpus=2)

    @ray.remote
    def f():
        ray.put(1)

    a = [f.remote() for _ in range(4)]  # noqa

    def verify():
        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            list_objects(raise_on_missing_output=False, _explain=True, timeout=5)
        return len(record) == 1

    wait_for_condition(verify)


@pytest.mark.asyncio
async def test_cli_format_print(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    actor_id = b"1234"
    data_source_client.get_all_actor_info.return_value = GetAllActorInfoReply(
        actor_table_data=[generate_actor_data(actor_id), generate_actor_data(b"12345")]
    )
    result = await state_api_manager.list_actors(option=create_api_options())
    print(result)
    result = [ActorState(**d) for d in result.result]
    # If the format is not yaml, it will raise an exception.
    yaml.safe_load(
        format_list_api_output(result, schema=ActorState, format=AvailableFormat.YAML)
    )
    # If the format is not json, it will raise an exception.
    json.loads(
        format_list_api_output(result, schema=ActorState, format=AvailableFormat.JSON)
    )
    # Test a table formatting.
    output = format_list_api_output(
        result, schema=ActorState, format=AvailableFormat.TABLE
    )
    assert "Table:" in output
    assert "Stats:" in output
    with pytest.raises(ValueError):
        format_list_api_output(result, schema=ActorState, format="random_format")

    # Verify the default format.
    output = format_list_api_output(result, schema=ActorState)
    assert "Table:" in output
    assert "Stats:" in output

    # Verify the ordering is equal to it is defined in `StateSchema` class.
    # Index 8 contains headers
    headers = output.split("\n")[8]
    cols = ActorState.list_columns()
    headers = list(filter(lambda item: item != "", headers.strip().split(" ")))

    for i in range(len(headers)):
        header = headers[i].upper()
        col = cols[i].upper()
        assert header == col


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_filter(shutdown_only):
    ray.init()

    # Test unsupported predicates.
    with pytest.raises(ValueError):
        list_actors(filters=[("state", ">", "DEAD")])

    @ray.remote
    class Actor:
        def __init__(self):
            self.obj = None

        def ready(self):
            pass

        def put(self):
            self.obj = ray.put(123)

        def getpid(self):
            import os

            return os.getpid()

    """
    Test basic case.
    """
    a = Actor.remote()
    b = Actor.remote()

    a_pid = ray.get(a.getpid.remote())
    b_pid = ray.get(b.getpid.remote())

    ray.get([a.ready.remote(), b.ready.remote()])
    ray.kill(b)

    def verify():
        result = list_actors(filters=[("state", "=", "DEAD")])
        assert len(result) == 1
        actor = result[0]
        assert actor["pid"] == b_pid

        result = list_actors(filters=[("state", "!=", "DEAD")])
        assert len(result) == 1
        actor = result[0]
        assert actor["pid"] == a_pid
        return True

    wait_for_condition(verify)

    """
    Test filter with different types (integer/bool).
    """
    obj_1 = ray.put(123)  # noqa
    ray.get(a.put.remote())
    pid = ray.get(a.getpid.remote())

    def verify():
        # There's only 1 object.
        result = list_objects(
            filters=[("pid", "=", pid), ("reference_type", "=", "LOCAL_REFERENCE")]
        )
        return len(result) == 1

    wait_for_condition(verify)

    def verify():
        workers = list_workers()
        live_workers = list_workers(
            filters=[("is_alive", "=", "true")], raise_on_missing_output=False
        )
        non_alive_workers = list_workers(
            filters=[("is_alive", "!=", "true")], raise_on_missing_output=False
        )
        assert len(live_workers) + len(non_alive_workers) == len(workers)

        live_workers = list_workers(
            filters=[("is_alive", "=", "1")], raise_on_missing_output=False
        )
        non_alive_workers = list_workers(
            filters=[("is_alive", "!=", "1")], raise_on_missing_output=False
        )
        assert len(live_workers) + len(non_alive_workers) == len(workers)

        live_workers = list_workers(
            filters=[("is_alive", "=", "True")], raise_on_missing_output=False
        )
        non_alive_workers = list_workers(
            filters=[("is_alive", "!=", "True")], raise_on_missing_output=False
        )
        assert len(live_workers) + len(non_alive_workers) == len(workers)

        return True

    wait_for_condition(verify)

    """
    Test CLI
    """
    dead_actor_id = list_actors(filters=[("state", "=", "DEAD")])[0]["actor_id"]
    alive_actor_id = list_actors(filters=[("state", "=", "ALIVE")])[0]["actor_id"]
    runner = CliRunner()
    result = runner.invoke(ray_list, ["actors", "--filter", "state=DEAD"])
    assert result.exit_code == 0
    assert dead_actor_id in result.output
    assert alive_actor_id not in result.output

    result = runner.invoke(ray_list, ["actors", "--filter", "state!=DEAD"])
    assert result.exit_code == 0
    assert dead_actor_id not in result.output
    assert alive_actor_id in result.output

    """
    Test case insensitive match on string fields.
    """

    @ray.remote
    def task():
        pass

    ray.get(task.remote())

    def verify():
        result_1 = list_tasks(filters=[("name", "=", "task")])
        result_2 = list_tasks(filters=[("name", "=", "TASK")])
        assert result_1 == result_2

        result_1 = list_tasks(filters=[("state", "=", "FINISHED")])
        result_2 = list_tasks(filters=[("state", "=", "finished")])
        assert result_1 == result_2

        result_1 = list_objects(
            filters=[("pid", "=", pid), ("reference_type", "=", "LOCAL_REFERENCE")]
        )

        result_2 = list_objects(
            filters=[("pid", "=", pid), ("reference_type", "=", "local_reference")]
        )
        assert result_1 == result_2

        result_1 = list_actors(filters=[("state", "=", "DEAD")])
        result_2 = list_actors(filters=[("state", "=", "dead")])

        assert result_1 == result_2

        result_1 = list_actors(filters=[("state", "!=", "DEAD")])
        result_2 = list_actors(filters=[("state", "!=", "dead")])

        assert result_1 == result_2
        return True

    wait_for_condition(verify)


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_data_truncate(shutdown_only, monkeypatch):
    """
    Verify the data is properly truncated when there are too many entries to return.
    """
    with monkeypatch.context() as m:
        max_limit_data_source = 10
        max_limit_api_server = 1000
        m.setenv("RAY_MAX_LIMIT_FROM_API_SERVER", f"{max_limit_api_server}")
        m.setenv("RAY_MAX_LIMIT_FROM_DATA_SOURCE", f"{max_limit_data_source}")

        ray.init(num_cpus=16)

        pgs = [  # noqa
            ray.util.placement_group(bundles=[{"CPU": 0.001}])
            for _ in range(max_limit_data_source + 1)
        ]
        runner = CliRunner()
        with pytest.warns(UserWarning) as record:
            result = runner.invoke(ray_list, ["placement-groups"])
        assert (
            f"{max_limit_data_source} ({max_limit_data_source + 1} total "
            "from the cluster) placement_groups are retrieved from the "
            "data source. 1 entries have been truncated." in record[0].message.args[0]
        )
        assert result.exit_code == 0

        # Make sure users cannot specify higher limit than MAX_LIMIT_FROM_API_SERVER
        with pytest.raises(RayStateApiException):
            list_placement_groups(limit=max_limit_api_server + 1)

        # TODO(rickyyx): We should support error code or more granular errors from
        # the server to the client so we could assert the specific type of error.
        # assert (
        #     f"Given limit {max_limit_api_server+1} exceeds the supported "
        #     f"limit {max_limit_api_server}." in str(e)
        # )

        # Make sure warning is not printed when truncation doesn't happen.
        @ray.remote
        class A:
            def ready(self):
                pass

        a = A.remote()
        ray.get(a.ready.remote())

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            result = runner.invoke(ray_list, ["actors"])
        assert len(record) == 0


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_detail(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    class Actor:
        def ready(self):
            pass

    a = Actor.remote()
    ray.get(a.ready.remote())

    """
    Test CLI
    """
    runner = CliRunner()
    result = runner.invoke(ray_list, ["actors", "--detail"])
    print(result.output)
    assert result.exit_code == 0
    # The column for --detail should be in the output.
    assert "test_detail" in result.output

    # Columns are upper case in the default formatting (table).
    assert "serialized_runtime_env" in result.output
    assert "actor_id" in result.output

    # Make sure when the --detail option is specified, the default formatting
    # is yaml. If the format is not yaml, the below line will raise an yaml exception.
    # Retrieve yaml content from result output
    print(yaml.safe_load(result.output.split("---")[1].split("...")[0]))

    # When the format is given, it should respect that formatting.
    result = runner.invoke(ray_list, ["actors", "--detail", "--format=json"])
    assert result.exit_code == 0
    # Fails if output is not JSON
    print(json.loads(result.output))


def _try_state_query_expect_rate_limit(api_func, res_q, start_q=None, **kwargs):
    """Utility functions for rate limit related e2e tests below"""
    try:
        # Indicate start of the process
        if start_q is not None:
            start_q.put(1)
        api_func(**kwargs)
    except RayStateApiException as e:
        # Other exceptions will be thrown
        if "Max number of in-progress requests" in str(e):
            res_q.put(1)
        else:
            res_q.put(e)
    except Exception as e:
        res_q.put(e)
    else:
        res_q.put(0)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Lambda test functions could not be pickled on Windows",
)
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_state_api_rate_limit_with_failure(monkeypatch, shutdown_only):
    import queue
    import threading

    # Set environment
    with monkeypatch.context() as m:
        m.setenv("RAY_STATE_SERVER_MAX_HTTP_REQUEST", "3")
        # These make list_nodes, list_workers, list_actors never return in 20secs
        m.setenv(
            "RAY_testing_asio_delay_us",
            (
                "TaskInfoGcsService.grpc_server.GetTaskEvents=20000000:20000000,"
                "WorkerInfoGcsService.grpc_server.GetAllWorkerInfo=20000000:20000000,"
                "ActorInfoGcsService.grpc_server.GetAllActorInfo=20000000:20000000"
            ),
        )

        # Set up scripts
        ray.init()

        @ray.remote
        def f():
            import time

            time.sleep(30)

        @ray.remote
        class Actor:
            pass

        task = f.remote()  # noqa
        actor = Actor.remote()  # noqa
        actor_runtime_env = Actor.options(  # noqa
            runtime_env={"pip": ["requests"]}
        ).remote()
        pg = ray.util.placement_group(bundles=[{"CPU": 1}])  # noqa

        _objs = [ray.put(x) for x in range(10)]  # noqa

        # Running 3 slow apis to exhaust the limits
        res_q = queue.Queue()
        start_q = queue.Queue()  # used for sync
        procs = [
            threading.Thread(
                target=_try_state_query_expect_rate_limit,
                args=(
                    list_workers,
                    res_q,
                    start_q,
                ),
                kwargs={"timeout": 6},
            ),
            threading.Thread(
                target=_try_state_query_expect_rate_limit,
                args=(
                    list_tasks,
                    res_q,
                    start_q,
                ),
                kwargs={"timeout": 6},
            ),
            threading.Thread(
                target=_try_state_query_expect_rate_limit,
                args=(
                    list_actors,
                    res_q,
                    start_q,
                ),
                kwargs={"timeout": 6},
            ),
        ]

        [p.start() for p in procs]

        # Wait for other processes to start so rate limit will be reached
        def _wait_to_start():
            started = 0
            for _ in range(3):
                started += start_q.get()
            return started == 3

        wait_for_condition(_wait_to_start)
        # Wait 1 more second to make sure the API call happens after all
        # process has a call.
        time.sleep(1)

        # Running another 1 should return error
        with pytest.raises(RayStateApiException) as e:
            print(list_objects())
        # TODO(rickyyx): We will use fine-grained exceptions/error code soon
        assert "Max" in str(
            e
        ), f"Expect an exception raised due to rate limit, but have {str(e)}"

        # Consecutive APIs should be successful after the previous delay ones timeout
        def verify():
            assert len(list_objects()) > 0, "non-delay APIs should be successful"
            "after previous ones timeout"

            return True

        wait_for_condition(verify)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Lambda test functions could not be pickled on Windows",
)
@pytest.mark.parametrize(
    "api_func",
    [
        # NOTE(rickyyx): arbitrary list of APIs, not exhaustive.
        list_objects,
        list_tasks,
        list_actors,
        list_nodes,
        list_placement_groups,
    ],
)
def test_state_api_server_enforce_concurrent_http_requests(
    api_func, monkeypatch, shutdown_only
):
    import queue
    import threading
    import time

    # Set environment
    with monkeypatch.context() as m:
        max_requests = 2
        m.setenv("RAY_STATE_SERVER_MAX_HTTP_REQUEST", str(max_requests))
        # All relevant calls delay to 2 secs
        m.setenv(
            "RAY_testing_asio_delay_us",
            (
                "TaskInfoGcsService.grpc_server.GetTaskEvents=200000:200000,"
                "NodeManagerService.grpc_server.GetObjectsInfo=200000:200000,"
                "ActorInfoGcsService.grpc_server.GetAllActorInfo=200000:200000,"
                "NodeInfoGcsService.grpc_server.GetAllNodeInfo=200000:200000,"
                "PlacementGroupInfoGcsService.grpc_server.GetAllPlacementGroup="
                "200000:200000"
            ),
        )

        ray.init()

        # Set up scripts
        @ray.remote
        def f():
            time.sleep(30)

        @ray.remote
        class Actor:
            pass

        task = f.remote()  # noqa
        actor = Actor.remote()  # noqa
        actor_runtime_env = Actor.options(  # noqa
            runtime_env={"pip": ["requests"]}
        ).remote()
        pg = ray.util.placement_group(bundles=[{"CPU": 1}])  # noqa

        _objs = [ray.put(x) for x in range(10)]  # noqa

        def verify():
            q = queue.Queue()
            num_procs = 3
            procs = [
                threading.Thread(
                    target=_try_state_query_expect_rate_limit,
                    args=(
                        api_func,
                        q,
                    ),
                )
                for _ in range(num_procs)
            ]

            [p.start() for p in procs]

            max_concurrent_reqs_error = 0
            for _ in range(num_procs):
                try:
                    res = q.get(timeout=10)
                    if isinstance(res, Exception):
                        assert False, f"State API error: {res}"
                    elif isinstance(res, int):
                        max_concurrent_reqs_error += res
                    else:
                        raise ValueError(res)
                except queue.Empty:
                    assert False, "Failed to get some results from a subprocess"

            # We should run into max in-progress requests errors
            assert (
                max_concurrent_reqs_error == num_procs - max_requests
            ), f"{num_procs - max_requests} requests should be rate limited"
            [p.join(5) for p in procs]
            for proc in procs:
                assert not proc.is_alive(), "All threads should exit"

            return True

        wait_for_condition(verify)


@pytest.mark.parametrize("callsite_enabled", [True, False])
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_callsite_warning(callsite_enabled, monkeypatch, shutdown_only):
    # Set environment
    with monkeypatch.context() as m:
        m.setenv("RAY_record_ref_creation_sites", str(int(callsite_enabled)))
        ray.init()

        a = ray.put(1)  # noqa

        runner = CliRunner()
        wait_for_condition(lambda: len(list_objects()) > 0)

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            result = runner.invoke(ray_list, ["objects"])
            assert result.exit_code == 0

        if callsite_enabled:
            assert len(record) == 0
        else:
            assert len(record) == 1
            assert "RAY_record_ref_creation_sites=1" in str(record[0].message)


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_raise_on_missing_output_partial_failures(monkeypatch, ray_start_cluster):
    """
    Verify when there are network partial failures,
    state API raises an exception when `raise_on_missing_output=True`.
    """
    monkeypatch.setenv("RAY_record_ref_creation_sites", "1")
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    with monkeypatch.context() as m:
        # defer for 10s for the second node.
        m.setenv(
            "RAY_testing_asio_delay_us",
            "NodeManagerService.grpc_server.GetObjectsInfo=10000000:10000000",
        )
        cluster.add_node(num_cpus=2)

    @ray.remote
    def f():
        ray.put(1)

    a = [f.remote() for _ in range(4)]  # noqa

    runner = CliRunner()

    # Verify
    def verify():
        # Verify when raise_on_missing_output=True, it raises an exception.
        try:
            list_objects(_explain=True, timeout=3)
        except RayStateApiException as e:
            assert "Failed to retrieve all objects from the cluster" in str(e)
            assert "due to query failures to the data sources." in str(e)
        else:
            assert False

        try:
            summarize_objects(_explain=True, timeout=3)
        except RayStateApiException as e:
            assert "Failed to retrieve all objects from the cluster" in str(e)
            assert "due to query failures to the data sources." in str(e)
        else:
            assert False

        # Verify when raise_on_missing_output=False, it prints warnings.
        with pytest.warns(UserWarning):
            list_objects(raise_on_missing_output=False, _explain=True, timeout=3)

        with pytest.warns(UserWarning):
            summarize_objects(raise_on_missing_output=False, _explain=True, timeout=3)

        # Verify when CLI is used, exceptions are not raised.
        with pytest.warns(UserWarning):
            result = runner.invoke(ray_list, ["objects", "--timeout=3"])
        assert result.exit_code == 0

        # Verify summary CLI also doesn't raise an exception.
        with pytest.warns(UserWarning):
            result = runner.invoke(summary_state_cli_group, ["objects", "--timeout=3"])
        assert result.exit_code == 0
        return True

    wait_for_condition(verify)


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_raise_on_missing_output_truncation(monkeypatch, shutdown_only):
    with monkeypatch.context() as m:
        # defer for 10s for the second node.
        m.setenv(
            "RAY_MAX_LIMIT_FROM_DATA_SOURCE",
            "10",
        )
        m.setenv(
            "RAY_task_events_skip_driver_for_test",
            "1",
        )
        ray.init()

        @ray.remote
        def task():
            time.sleep(300)

        tasks = [task.remote() for _ in range(15)]  # noqa

    runner = CliRunner()

    # Verify
    def verify():
        # Verify when raise_on_missing_output=True, it raises an exception.
        try:
            list_tasks(_explain=True, timeout=3)
        except RayStateApiException as e:
            assert "Failed to retrieve all" in str(e)
            assert "(> 10)" in str(e)
        else:
            assert False

        try:
            summarize_tasks(_explain=True, timeout=3)
        except RayStateApiException as e:
            assert "Failed to retrieve all" in str(e)
            assert "(> 10)" in str(e)
        else:
            assert False

        # Verify when raise_on_missing_output=False, it prints warnings.
        with pytest.warns(UserWarning):
            list_tasks(raise_on_missing_output=False, _explain=True, timeout=3)

        with pytest.warns(UserWarning):
            summarize_tasks(raise_on_missing_output=False, _explain=True, timeout=3)

        # Verify when CLI is used, exceptions are not raised.
        with pytest.warns(UserWarning):
            result = runner.invoke(ray_list, ["tasks", "--timeout=3"])
        assert result.exit_code == 0

        # Verify summary CLI also doesn't raise an exception.
        with pytest.warns(UserWarning):
            result = runner.invoke(summary_state_cli_group, ["tasks", "--timeout=3"])
        assert result.exit_code == 0
        return True

    wait_for_condition(verify)


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_core_state_api_usage_tags(shutdown_only):
    from ray._common.usage.usage_lib import TagKey, get_extra_usage_tags_to_report

    ctx = ray.init()
    gcs_client = GcsClient(address=ctx.address_info["gcs_address"])
    list_actors()
    list_tasks()
    list_jobs()
    list_cluster_events()
    list_nodes()
    list_objects()
    list_runtime_envs()
    list_workers()

    summarize_actors()
    summarize_objects()
    summarize_tasks()

    result = get_extra_usage_tags_to_report(gcs_client)

    expected_tags = [
        TagKey.CORE_STATE_API_LIST_ACTORS,
        TagKey.CORE_STATE_API_LIST_TASKS,
        TagKey.CORE_STATE_API_LIST_JOBS,
        TagKey.CORE_STATE_API_LIST_CLUSTER_EVENTS,
        TagKey.CORE_STATE_API_LIST_NODES,
        TagKey.CORE_STATE_API_LIST_OBJECTS,
        TagKey.CORE_STATE_API_LIST_RUNTIME_ENVS,
        TagKey.CORE_STATE_API_LIST_WORKERS,
        TagKey.CORE_STATE_API_SUMMARIZE_ACTORS,
        TagKey.CORE_STATE_API_SUMMARIZE_OBJECTS,
        TagKey.CORE_STATE_API_SUMMARIZE_TASKS,
    ]
    assert set(result.keys()).issuperset(
        {TagKey.Name(tag).lower() for tag in expected_tags}
    )


# Tests fix for https://github.com/ray-project/ray/issues/44459
def test_job_info_is_running_task(shutdown_only):
    ray.init()

    # To reliably know a job has a long running task, we need to wait a SignalActor
    # to know the task has started.
    signal = SignalActor.remote()

    @ray.remote
    def f(signal):
        ray.get(signal.send.remote())
        import time

        while True:
            time.sleep(10000)

    long_running = f.remote(signal)  # noqa: F841
    ray.get(signal.wait.remote())

    client = ray.worker.global_worker.gcs_client
    job_id = ray.worker.global_worker.current_job_id
    all_job_info = client.get_all_job_info()
    assert len(all_job_info) == 1
    assert job_id in all_job_info
    assert all_job_info[job_id].is_running_tasks is True


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_hang_driver_has_no_is_running_task(monkeypatch, ray_start_cluster):
    """
    When there's a call to JobInfoGcsService.GetAllJobInfo, GCS sends RPC
    CoreWorkerService.NumPendingTasks to all drivers for "is_running_task". Our driver
    however has trouble serving such RPC, and GCS should timeout that RPC and unsest the
    field.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=10)
    address = cluster.address

    monkeypatch.setenv(
        "RAY_testing_asio_delay_us",
        "CoreWorkerService.grpc_server.NumPendingTasks=2000000:2000000",
    )
    ray.init(address=address)

    client = ray.worker.global_worker.gcs_client
    my_job_id = ray.worker.global_worker.current_job_id
    all_job_info = client.get_all_job_info()
    assert list(all_job_info.keys()) == [my_job_id]
    assert not all_job_info[my_job_id].HasField("is_running_tasks")


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
