import time
import json
import sys
from collections import Counter
from dataclasses import dataclass
from typing import List, Tuple
from unittest.mock import MagicMock

import pytest
from ray._private import gcs_utils
from ray._private.gcs_utils import GcsAioClient
import yaml
from click.testing import CliRunner

import ray
import ray.dashboard.consts as dashboard_consts
import ray._private.state as global_state
import ray._private.ray_constants as ray_constants
from ray._private.test_utils import (
    wait_for_condition,
    async_wait_for_condition_async_predicate,
)
from ray.cluster_utils import cluster_not_supported
from ray._raylet import NodeID
from ray.core.generated.common_pb2 import (
    Address,
    CoreWorkerStats,
    ObjectRefInfo,
    TaskInfoEntry,
    TaskStatus,
    WorkerType,
    TaskType,
)
from ray.core.generated.gcs_pb2 import (
    TaskEvents,
    TaskStateUpdate,
    ActorTableData,
    GcsNodeInfo,
    PlacementGroupTableData,
    WorkerTableData,
)
from ray.core.generated.gcs_service_pb2 import (
    GcsStatus,
    GetTaskEventsReply,
    GetAllActorInfoReply,
    GetAllNodeInfoReply,
    GetAllPlacementGroupReply,
    GetAllWorkerInfoReply,
)
from ray.core.generated.node_manager_pb2 import GetTasksInfoReply, GetObjectsInfoReply
from ray.core.generated.reporter_pb2 import ListLogsReply, StreamLogReply
from ray.core.generated.runtime_env_agent_pb2 import GetRuntimeEnvsInfoReply
from ray.core.generated.runtime_env_common_pb2 import (
    RuntimeEnvState as RuntimeEnvStateProto,
)
from ray.dashboard.state_aggregator import (
    GCS_QUERY_FAILURE_WARNING,
    NODE_QUERY_FAILURE_WARNING,
    StateAPIManager,
    _convert_filters_type,
)
from ray.experimental.state.api import (
    get_actor,
    get_node,
    get_objects,
    get_placement_group,
    get_task,
    get_worker,
    list_actors,
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
    list_cluster_events,
    StateApiClient,
)
from ray._private.event.event_logger import get_event_id
from ray.experimental.state.common import (
    DEFAULT_LIMIT,
    DEFAULT_RPC_TIMEOUT,
    ActorState,
    ListApiOptions,
    SummaryApiOptions,
    NodeState,
    ObjectState,
    PlacementGroupState,
    RuntimeEnvState,
    SupportedFilterType,
    TaskState,
    WorkerState,
    ClusterEventState,
    StateSchema,
    state_column,
)
from ray.dashboard.utils import ray_address_to_api_server_url
from ray.experimental.state.exception import DataSourceUnavailable, RayStateApiException
from ray.experimental.state.state_cli import (
    AvailableFormat,
    format_list_api_output,
    _parse_filter,
    summary_state_cli_group,
)
from ray.experimental.state.state_cli import ray_get
from ray.experimental.state.state_cli import ray_list
from ray.experimental.state.state_manager import IdToIpMap, StateDataSourceClient
from ray.job_submission import JobSubmissionClient
from ray.runtime_env import RuntimeEnv

if sys.version_info >= (3, 8, 0):
    from unittest.mock import AsyncMock
else:
    from asyncmock import AsyncMock


"""
Unit tests
"""


@pytest.fixture
def state_api_manager():
    data_source_client = AsyncMock(StateDataSourceClient)
    manager = StateAPIManager(data_source_client)
    yield manager


def verify_schema(state, result_dict: dict, detail: bool = False):
    state_fields_columns = set()
    if detail:
        state_fields_columns = state.columns()
    else:
        state_fields_columns = state.base_columns()

    for k in state_fields_columns:
        assert k in result_dict

    for k in result_dict:
        assert k in state_fields_columns


def generate_actor_data(id, state=ActorTableData.ActorState.ALIVE, class_name="class"):
    return ActorTableData(
        actor_id=id,
        state=state,
        name="abc",
        pid=1234,
        class_name=class_name,
        address=Address(raylet_id=id, ip_address="127.0.0.1", port=124, worker_id=id),
    )


def generate_pg_data(id):
    return PlacementGroupTableData(
        placement_group_id=id,
        state=PlacementGroupTableData.PlacementGroupState.CREATED,
        name="abc",
        creator_job_dead=True,
        creator_actor_dead=False,
    )


def generate_node_data(id):
    return GcsNodeInfo(
        node_id=id,
        state=GcsNodeInfo.GcsNodeState.ALIVE,
        node_manager_address="127.0.0.1",
        raylet_socket_name="abcd",
        object_store_socket_name="False",
    )


def generate_worker_data(id, pid=1234):
    return WorkerTableData(
        worker_address=Address(
            raylet_id=id, ip_address="127.0.0.1", port=124, worker_id=id
        ),
        is_alive=True,
        timestamp=1234,
        worker_type=WorkerType.WORKER,
        pid=pid,
        exit_type=None,
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
    )
    setattr(state_updates, TaskStatus.Name(state).lower() + "_ts", 1)
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


def generate_runtime_env_info(runtime_env, creation_time=None, success=True):
    return GetRuntimeEnvsInfoReply(
        runtime_env_states=[
            RuntimeEnvStateProto(
                runtime_env=runtime_env.serialize(),
                ref_cnt=1,
                success=success,
                error=None,
                creation_time_ms=creation_time,
            )
        ],
        total=1,
    )


def create_api_options(
    timeout: int = DEFAULT_RPC_TIMEOUT,
    limit: int = DEFAULT_LIMIT,
    filters: List[Tuple[str, SupportedFilterType]] = None,
    detail: bool = False,
):
    if not filters:
        filters = []
    return ListApiOptions(
        limit=limit,
        timeout=timeout,
        filters=filters,
        _server_timeout_multiplier=1.0,
        detail=detail,
    )


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
    gcs_port = gcs_address.split(":")[1]
    assert api_server_url == ray_address_to_api_server_url(f"localhost:{gcs_port}")


def test_state_schema():
    @dataclass
    class TestSchema(StateSchema):
        column_a: int
        column_b: int = state_column(filterable=False)
        column_c: int = state_column(filterable=True)
        column_d: int = state_column(filterable=False, detail=False)
        column_e: int = state_column(filterable=False, detail=True)
        column_f: int = state_column(filterable=True, detail=False)
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
    with pytest.raises(AssertionError):
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


def test_id_to_ip_map():
    node_id_1 = "1"
    node_ip_1 = "ip_1"
    node_id_2 = "2"
    node_ip_2 = "ip_2"
    m = IdToIpMap()
    m.put(node_id_1, node_ip_1)
    assert m.get_ip(node_ip_2) is None
    assert m.get_node_id(node_id_2) is None
    assert m.get_ip(node_id_1) == node_ip_1
    assert m.get_node_id(node_ip_1) == node_id_1
    m.pop(node_id_1)
    assert m.get_ip(node_id_1) is None
    assert m.get_node_id(node_id_1) is None


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
async def test_api_manager_list_actors(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    actor_id = b"1234"
    data_source_client.get_all_actor_info.return_value = GetAllActorInfoReply(
        actor_table_data=[
            generate_actor_data(actor_id),
            generate_actor_data(b"12345", state=ActorTableData.ActorState.DEAD),
        ],
        total=2,
    )
    result = await state_api_manager.list_actors(option=create_api_options())
    data = result.result

    actor_data = data[0]
    verify_schema(ActorState, actor_data)
    assert result.total == 2

    """
    Test detail
    """
    result = await state_api_manager.list_actors(option=create_api_options(detail=True))
    data = result.result
    actor_data = data[0]
    verify_schema(ActorState, actor_data, detail=True)

    """
    Test limit
    """
    assert len(data) == 2
    result = await state_api_manager.list_actors(option=create_api_options(limit=1))
    data = result.result
    assert len(data) == 1
    assert result.total == 2

    """
    Test filters
    """
    # If the column is not supported for filtering, it should raise an exception.
    with pytest.raises(ValueError):
        result = await state_api_manager.list_actors(
            option=create_api_options(filters=[("stat", "=", "DEAD")])
        )
    result = await state_api_manager.list_actors(
        option=create_api_options(filters=[("state", "=", "DEAD")])
    )
    assert len(result.result) == 1

    """
    Test error handling
    """
    data_source_client.get_all_actor_info.side_effect = DataSourceUnavailable()
    with pytest.raises(DataSourceUnavailable) as exc_info:
        result = await state_api_manager.list_actors(option=create_api_options(limit=1))
    assert exc_info.value.args[0] == GCS_QUERY_FAILURE_WARNING


@pytest.mark.asyncio
async def test_api_manager_list_pgs(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    id = b"1234"
    data_source_client.get_all_placement_group_info.return_value = (
        GetAllPlacementGroupReply(
            placement_group_table_data=[
                generate_pg_data(id),
                generate_pg_data(b"12345"),
            ],
            total=2,
        )
    )
    result = await state_api_manager.list_placement_groups(option=create_api_options())
    data = result.result
    data = data[0]
    verify_schema(PlacementGroupState, data)
    assert result.total == 2

    """
    Test detail
    """
    result = await state_api_manager.list_placement_groups(
        option=create_api_options(detail=True)
    )
    data = result.result
    data = data[0]
    verify_schema(PlacementGroupState, data, detail=True)

    """
    Test limit
    """
    assert len(result.result) == 2
    result = await state_api_manager.list_placement_groups(
        option=create_api_options(limit=1)
    )
    data = result.result
    assert len(data) == 1
    assert result.total == 2

    """
    Test filters
    """
    # If the column is not supported for filtering, it should raise an exception.
    with pytest.raises(ValueError):
        result = await state_api_manager.list_placement_groups(
            option=create_api_options(filters=[("stat", "=", "DEAD")])
        )
    result = await state_api_manager.list_placement_groups(
        option=create_api_options(
            filters=[("placement_group_id", "=", bytearray(id).hex())]
        )
    )
    assert len(result.result) == 1

    """
    Test error handling
    """
    data_source_client.get_all_placement_group_info.side_effect = (
        DataSourceUnavailable()
    )
    with pytest.raises(DataSourceUnavailable) as exc_info:
        result = await state_api_manager.list_placement_groups(
            option=create_api_options(limit=1)
        )
    assert exc_info.value.args[0] == GCS_QUERY_FAILURE_WARNING


@pytest.mark.asyncio
async def test_api_manager_list_cluster_events(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    event_id_1 = get_event_id()
    event_id_2 = get_event_id()
    data_source_client.get_all_cluster_events.return_value = {
        "job_1": {
            event_id_1: {
                "timestamp": 10,
                "severity": "DEBUG",
                "message": "a",
                "event_id": event_id_1,
            },
            event_id_2: {
                "timestamp": 10,
                "severity": "INFO",
                "message": "b",
                "event_id": event_id_2,
            },
        }
    }
    result = await state_api_manager.list_cluster_events(option=create_api_options())
    data = result.result
    data = data[0]
    verify_schema(ClusterEventState, data)
    assert result.total == 2

    """
    Test detail
    """
    # TODO(sang)

    """
    Test limit
    """
    assert len(result.result) == 2
    result = await state_api_manager.list_cluster_events(
        option=create_api_options(limit=1)
    )
    data = result.result
    assert len(data) == 1
    assert result.total == 2

    """
    Test filters
    """
    # If the column is not supported for filtering, it should raise an exception.
    with pytest.raises(ValueError):
        result = await state_api_manager.list_cluster_events(
            option=create_api_options(filters=[("time", "=", "20")])
        )
    result = await state_api_manager.list_cluster_events(
        option=create_api_options(filters=[("severity", "=", "INFO")])
    )
    assert len(result.result) == 1


@pytest.mark.asyncio
async def test_api_manager_list_nodes(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    id = b"1234"
    data_source_client.get_all_node_info.return_value = GetAllNodeInfoReply(
        node_info_list=[generate_node_data(id), generate_node_data(b"12345")]
    )
    result = await state_api_manager.list_nodes(option=create_api_options())
    data = result.result
    data = data[0]
    verify_schema(NodeState, data)
    assert result.total == 2

    """
    Test detail
    """
    result = await state_api_manager.list_nodes(option=create_api_options(detail=True))
    data = result.result
    data = data[0]
    verify_schema(NodeState, data, detail=True)

    """
    Test limit
    """
    assert len(result.result) == 2
    result = await state_api_manager.list_nodes(option=create_api_options(limit=1))
    data = result.result
    assert len(data) == 1
    assert result.total == 2

    """
    Test filters
    """
    # If the column is not supported for filtering, it should raise an exception.
    with pytest.raises(ValueError):
        result = await state_api_manager.list_nodes(
            option=create_api_options(filters=[("stat", "=", "DEAD")])
        )
    result = await state_api_manager.list_nodes(
        option=create_api_options(filters=[("node_id", "=", bytearray(id).hex())])
    )
    assert len(result.result) == 1

    """
    Test error handling
    """
    data_source_client.get_all_node_info.side_effect = DataSourceUnavailable()
    with pytest.raises(DataSourceUnavailable) as exc_info:
        result = await state_api_manager.list_nodes(option=create_api_options(limit=1))
    assert exc_info.value.args[0] == GCS_QUERY_FAILURE_WARNING


@pytest.mark.asyncio
async def test_api_manager_list_workers(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    id = b"1234"
    data_source_client.get_all_worker_info.return_value = GetAllWorkerInfoReply(
        worker_table_data=[
            generate_worker_data(id, pid=1),
            generate_worker_data(b"12345", pid=2),
        ],
        total=2,
    )
    result = await state_api_manager.list_workers(option=create_api_options())
    data = result.result
    data = data[0]
    verify_schema(WorkerState, data)
    assert result.total == 2

    """
    Test detail
    """
    result = await state_api_manager.list_workers(
        option=create_api_options(detail=True)
    )
    data = result.result
    data = data[0]
    verify_schema(WorkerState, data, detail=True)

    """
    Test limit
    """
    assert len(result.result) == 2
    result = await state_api_manager.list_workers(option=create_api_options(limit=1))
    data = result.result
    assert len(data) == 1
    assert result.total == 2

    """
    Test filters
    """
    # If the column is not supported for filtering, it should raise an exception.
    with pytest.raises(ValueError):
        result = await state_api_manager.list_workers(
            option=create_api_options(filters=[("stat", "=", "DEAD")])
        )
    result = await state_api_manager.list_workers(
        option=create_api_options(filters=[("worker_id", "=", bytearray(id).hex())])
    )
    assert len(result.result) == 1
    # Make sure it works with int type.
    result = await state_api_manager.list_workers(
        option=create_api_options(filters=[("pid", "=", 2)])
    )
    assert len(result.result) == 1

    """
    Test error handling
    """
    data_source_client.get_all_worker_info.side_effect = DataSourceUnavailable()
    with pytest.raises(DataSourceUnavailable) as exc_info:
        result = await state_api_manager.list_workers(
            option=create_api_options(limit=1)
        )
    assert exc_info.value.args[0] == GCS_QUERY_FAILURE_WARNING


@pytest.mark.skipif(
    sys.version_info < (3, 8, 0),
    reason=("Not passing in CI although it works locally. Will handle it later."),
)
@pytest.mark.asyncio
async def test_api_manager_list_tasks(state_api_manager):
    data_source_client = state_api_manager.data_source_client

    node_id = NodeID.from_random()
    first_task_name = "1"
    second_task_name = "2"
    data_source_client.get_all_task_info = AsyncMock()
    id = b"1234"
    data_source_client.get_all_task_info.side_effect = [
        generate_task_data(
            [
                generate_task_event(id, first_task_name, node_id=node_id),
                generate_task_event(b"2345", second_task_name, node_id=None),
            ]
        )
    ]
    result = await state_api_manager.list_tasks(option=create_api_options())
    data_source_client.get_all_task_info.assert_any_await(
        timeout=DEFAULT_RPC_TIMEOUT, job_id=None
    )
    data = result.result
    data = data
    assert len(data) == 2
    assert result.total == 2
    verify_schema(TaskState, data[0])
    assert data[0]["node_id"] == node_id.hex()
    verify_schema(TaskState, data[1])
    assert data[1]["node_id"] is None

    """
    Test detail
    """
    data_source_client.get_all_task_info.side_effect = [
        generate_task_data(
            [
                generate_task_event(id, first_task_name),
                generate_task_event(b"2345", second_task_name),
            ]
        )
    ]
    result = await state_api_manager.list_tasks(option=create_api_options(detail=True))
    data = result.result
    data = data
    verify_schema(TaskState, data[0], detail=True)
    verify_schema(TaskState, data[1], detail=True)

    """
    Test limit
    """
    data_source_client.get_all_task_info.side_effect = [
        generate_task_data(
            [
                generate_task_event(id, first_task_name),
                generate_task_event(b"2345", second_task_name),
            ]
        )
    ]
    result = await state_api_manager.list_tasks(option=create_api_options(limit=1))
    data = result.result
    assert len(data) == 1
    assert result.total == 2

    """
    Test filters
    """
    data_source_client.get_all_task_info.side_effect = [
        generate_task_data(
            [
                generate_task_event(id, first_task_name),
                generate_task_event(b"2345", second_task_name),
            ]
        )
    ]
    result = await state_api_manager.list_tasks(
        option=create_api_options(filters=[("task_id", "=", bytearray(id).hex())])
    )
    assert len(result.result) == 1


@pytest.mark.skipif(
    sys.version_info < (3, 8, 0),
    reason=("Not passing in CI although it works locally. Will handle it later."),
)
@pytest.mark.asyncio
async def test_api_manager_list_tasks_events(state_api_manager):
    data_source_client = state_api_manager.data_source_client

    node_id = NodeID.from_random()
    data_source_client.get_all_task_info = AsyncMock()
    id = b"1234"
    func_or_class = "f"

    # Generate a task event.

    task_info = TaskInfoEntry(
        task_id=id,
        name=func_or_class,
        func_or_class_name=func_or_class,
        type=TaskType.NORMAL_TASK,
    )
    current = time.time_ns()
    second = int(1e9)
    state_updates = TaskStateUpdate(
        node_id=node_id.binary(),
        pending_args_avail_ts=current,
        submitted_to_worker_ts=current + second,
        running_ts=current + (2 * second),
        finished_ts=current + (3 * second),
    )

    """
    Test basic.
    """
    events = TaskEvents(
        task_id=id,
        job_id=b"0001",
        attempt_number=0,
        task_info=task_info,
        state_updates=state_updates,
    )
    data_source_client.get_all_task_info.side_effect = [generate_task_data([events])]
    result = await state_api_manager.list_tasks(option=create_api_options(detail=True))
    result = result.result[0]
    assert "events" in result
    assert result["state"] == "FINISHED"
    expected_events = [
        {
            "state": "PENDING_ARGS_AVAIL",
            "created_ms": current // 1e6,
        },
        {
            "state": "SUBMITTED_TO_WORKER",
            "created_ms": (current + second) // 1e6,
        },
        {
            "state": "RUNNING",
            "created_ms": (current + 2 * second) // 1e6,
        },
        {
            "state": "FINISHED",
            "created_ms": (current + 3 * second) // 1e6,
        },
    ]
    for actual, expected in zip(result["events"], expected_events):
        assert actual == expected
    assert result["start_time_ms"] == (current + 2 * second) // 1e6
    assert result["end_time_ms"] == (current + 3 * second) // 1e6

    """
    Test only start_time_ms is updated.
    """
    state_updates = TaskStateUpdate(
        node_id=node_id.binary(),
        pending_args_avail_ts=current,
        submitted_to_worker_ts=current + second,
        running_ts=current + (2 * second),
    )
    events = TaskEvents(
        task_id=id,
        job_id=b"0001",
        attempt_number=0,
        task_info=task_info,
        state_updates=state_updates,
    )
    data_source_client.get_all_task_info.side_effect = [generate_task_data([events])]
    result = await state_api_manager.list_tasks(option=create_api_options(detail=True))
    result = result.result[0]
    assert result["start_time_ms"] == (current + 2 * second) // 1e6
    assert result["end_time_ms"] is None

    """
    Test None of start & end time is updated.
    """
    state_updates = TaskStateUpdate(
        pending_args_avail_ts=current,
        submitted_to_worker_ts=current + second,
    )
    events = TaskEvents(
        task_id=id,
        job_id=b"0001",
        attempt_number=0,
        task_info=task_info,
        state_updates=state_updates,
    )
    data_source_client.get_all_task_info.side_effect = [generate_task_data([events])]
    result = await state_api_manager.list_tasks(option=create_api_options(detail=True))
    result = result.result[0]
    assert result["start_time_ms"] is None
    assert result["end_time_ms"] is None


@pytest.mark.skipif(
    sys.version_info < (3, 8, 0),
    reason=("Not passing in CI although it works locally. Will handle it later."),
)
@pytest.mark.asyncio
async def test_api_manager_summarize_tasks(state_api_manager):
    data_source_client = state_api_manager.data_source_client

    node_id = NodeID.from_random()
    first_task_name = "1"
    second_task_name = "2"
    data_source_client.get_all_task_info = AsyncMock()
    id = b"1234"
    data_source_client.get_all_task_info.side_effect = [
        generate_task_data(
            [
                generate_task_event(
                    id, first_task_name, func_or_class=first_task_name, node_id=node_id
                ),
                generate_task_event(
                    b"2345",
                    first_task_name,
                    func_or_class=first_task_name,
                    node_id=node_id,
                ),
                generate_task_event(
                    b"3456",
                    second_task_name,
                    func_or_class=second_task_name,
                    node_id=None,
                ),
                generate_task_event(
                    b"4567",
                    first_task_name,
                    func_or_class=first_task_name,
                    node_id=node_id,
                    job_id=b"0002",
                ),
            ]
        )
    ]
    result = await state_api_manager.summarize_tasks(option=SummaryApiOptions())
    data = result.result.node_id_to_summary["cluster"].summary
    assert len(data) == 2  # 2 task names
    assert result.total == 4  # 4 total tasks

    assert data[first_task_name].state_counts["PENDING_NODE_ASSIGNMENT"] == 3
    assert data[second_task_name].state_counts["PENDING_NODE_ASSIGNMENT"] == 1

    """
    With job_id filter
    """
    data_source_client.get_all_task_info.side_effect = [
        generate_task_data(
            [
                generate_task_event(
                    id, first_task_name, func_or_class=first_task_name, node_id=node_id
                ),
                generate_task_event(
                    b"2345",
                    first_task_name,
                    func_or_class=first_task_name,
                    node_id=node_id,
                ),
                generate_task_event(
                    b"3456",
                    second_task_name,
                    func_or_class=second_task_name,
                    node_id=None,
                ),
                generate_task_event(
                    b"4567",
                    first_task_name,
                    func_or_class=first_task_name,
                    node_id=node_id,
                    job_id=b"0002",
                ),
            ]
        )
    ]
    result = await state_api_manager.summarize_tasks(
        option=SummaryApiOptions(filters=[("job_id", "=", b"0002".hex())])
    )
    data = result.result.node_id_to_summary["cluster"].summary
    assert len(data) == 1  # 1 task name
    assert result.total == 4  # 4 total task (across all jobs)
    assert result.num_filtered == 1  # 1 total task (for single job)

    assert data[first_task_name].state_counts["PENDING_NODE_ASSIGNMENT"] == 1


@pytest.mark.skipif(
    sys.version_info < (3, 8, 0),
    reason=("Not passing in CI although it works locally. Will handle it later."),
)
@pytest.mark.asyncio
async def test_api_manager_list_objects(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    obj_1_id = b"1" * 28
    obj_2_id = b"2" * 28
    data_source_client.get_all_registered_raylet_ids = MagicMock()
    data_source_client.get_all_registered_raylet_ids.return_value = ["1", "2"]

    data_source_client.get_object_info = AsyncMock()
    data_source_client.get_object_info.side_effect = [
        GetObjectsInfoReply(
            core_workers_stats=[generate_object_info(obj_1_id)], total=1
        ),
        GetObjectsInfoReply(
            core_workers_stats=[generate_object_info(obj_2_id)], total=1
        ),
    ]
    result = await state_api_manager.list_objects(option=create_api_options())
    data = result.result
    data_source_client.get_object_info.assert_any_await(
        "1", timeout=DEFAULT_RPC_TIMEOUT
    )
    data_source_client.get_object_info.assert_any_await(
        "2", timeout=DEFAULT_RPC_TIMEOUT
    )
    data = data
    assert len(data) == 2
    verify_schema(ObjectState, data[0])
    verify_schema(ObjectState, data[1])
    assert result.total == 2

    """
    Test detail
    """
    data_source_client.get_object_info.side_effect = [
        GetObjectsInfoReply(
            core_workers_stats=[generate_object_info(obj_1_id)], total=1
        ),
        GetObjectsInfoReply(
            core_workers_stats=[generate_object_info(obj_2_id)], total=1
        ),
    ]
    result = await state_api_manager.list_objects(
        option=create_api_options(detail=True)
    )
    data = result.result
    data = data
    verify_schema(ObjectState, data[0], detail=True)
    verify_schema(ObjectState, data[1], detail=True)

    """
    Test limit
    """
    data_source_client.get_object_info.side_effect = [
        GetObjectsInfoReply(
            core_workers_stats=[generate_object_info(obj_1_id)], total=1
        ),
        GetObjectsInfoReply(
            core_workers_stats=[generate_object_info(obj_2_id)], total=1
        ),
    ]
    result = await state_api_manager.list_objects(option=create_api_options(limit=1))
    data = result.result
    assert len(data) == 1
    assert result.total == 2

    """
    Test filters
    """
    data_source_client.get_object_info.side_effect = [
        GetObjectsInfoReply(core_workers_stats=[generate_object_info(obj_1_id)]),
        GetObjectsInfoReply(core_workers_stats=[generate_object_info(obj_2_id)]),
    ]
    result = await state_api_manager.list_objects(
        option=create_api_options(
            filters=[("object_id", "=", bytearray(obj_1_id).hex())]
        )
    )
    assert len(result.result) == 1

    """
    Test error handling
    """
    data_source_client.get_object_info.side_effect = [
        DataSourceUnavailable(),
        GetObjectsInfoReply(core_workers_stats=[generate_object_info(obj_2_id)]),
    ]
    result = await state_api_manager.list_objects(option=create_api_options(limit=1))
    # Make sure warnings are returned.
    warning = result.partial_failure_warning
    assert (
        NODE_QUERY_FAILURE_WARNING.format(
            type="raylet", total=2, network_failures=1, log_command="raylet.out"
        )
        in warning
    )

    # Test if all RPCs fail, it will raise an exception.
    data_source_client.get_object_info.side_effect = [
        DataSourceUnavailable(),
        DataSourceUnavailable(),
    ]
    with pytest.raises(DataSourceUnavailable):
        result = await state_api_manager.list_objects(
            option=create_api_options(limit=1)
        )


@pytest.mark.skipif(
    sys.version_info < (3, 8, 0),
    reason=("Not passing in CI although it works locally. Will handle it later."),
)
@pytest.mark.asyncio
async def test_api_manager_list_runtime_envs(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    data_source_client.get_all_registered_agent_ids = MagicMock()
    data_source_client.get_all_registered_agent_ids.return_value = ["1", "2", "3"]

    data_source_client.get_runtime_envs_info = AsyncMock()
    data_source_client.get_runtime_envs_info.side_effect = [
        generate_runtime_env_info(RuntimeEnv(**{"pip": ["requests"]})),
        generate_runtime_env_info(
            RuntimeEnv(**{"pip": ["tensorflow"]}), creation_time=15
        ),
        generate_runtime_env_info(RuntimeEnv(**{"pip": ["ray"]}), creation_time=10),
    ]
    result = await state_api_manager.list_runtime_envs(option=create_api_options())
    data = result.result
    data_source_client.get_runtime_envs_info.assert_any_await(
        "1", timeout=DEFAULT_RPC_TIMEOUT
    )
    data_source_client.get_runtime_envs_info.assert_any_await(
        "2", timeout=DEFAULT_RPC_TIMEOUT
    )

    data_source_client.get_runtime_envs_info.assert_any_await(
        "3", timeout=DEFAULT_RPC_TIMEOUT
    )
    assert len(data) == 3
    verify_schema(RuntimeEnvState, data[0])
    verify_schema(RuntimeEnvState, data[1])
    verify_schema(RuntimeEnvState, data[2])
    assert result.total == 3

    # Make sure the higher creation time is sorted first.
    data[1]["creation_time_ms"] > data[2]["creation_time_ms"]

    """
    Test detail
    """
    data_source_client.get_runtime_envs_info.side_effect = [
        generate_runtime_env_info(RuntimeEnv(**{"pip": ["requests"]})),
        generate_runtime_env_info(
            RuntimeEnv(**{"pip": ["tensorflow"]}), creation_time=15
        ),
        generate_runtime_env_info(RuntimeEnv(**{"pip": ["ray"]}), creation_time=10),
    ]
    result = await state_api_manager.list_runtime_envs(
        option=create_api_options(detail=True)
    )
    data = result.result
    verify_schema(RuntimeEnvState, data[0], detail=True)
    verify_schema(RuntimeEnvState, data[1], detail=True)
    verify_schema(RuntimeEnvState, data[2], detail=True)

    """
    Test limit
    """
    data_source_client.get_runtime_envs_info.side_effect = [
        generate_runtime_env_info(RuntimeEnv(**{"pip": ["requests"]})),
        generate_runtime_env_info(
            RuntimeEnv(**{"pip": ["tensorflow"]}), creation_time=15
        ),
        generate_runtime_env_info(RuntimeEnv(**{"pip": ["ray"]})),
    ]
    result = await state_api_manager.list_runtime_envs(
        option=create_api_options(limit=1)
    )
    data = result.result
    assert len(data) == 1
    assert result.total == 3

    """
    Test filters
    """
    data_source_client.get_runtime_envs_info.side_effect = [
        generate_runtime_env_info(RuntimeEnv(**{"pip": ["requests"]}), success=True),
        generate_runtime_env_info(
            RuntimeEnv(**{"pip": ["tensorflow"]}), creation_time=15, success=True
        ),
        generate_runtime_env_info(RuntimeEnv(**{"pip": ["ray"]}), success=False),
    ]
    result = await state_api_manager.list_runtime_envs(
        option=create_api_options(filters=[("success", "=", False)])
    )
    assert len(result.result) == 1

    """
    Test error handling
    """
    data_source_client.get_runtime_envs_info.side_effect = [
        DataSourceUnavailable(),
        generate_runtime_env_info(RuntimeEnv(**{"pip": ["ray"]})),
        generate_runtime_env_info(RuntimeEnv(**{"pip": ["ray"]})),
    ]
    result = await state_api_manager.list_runtime_envs(
        option=create_api_options(limit=1)
    )
    # Make sure warnings are returned.
    warning = result.partial_failure_warning
    assert (
        NODE_QUERY_FAILURE_WARNING.format(
            type="agent", total=3, network_failures=1, log_command="dashboard_agent.log"
        )
        in warning
    )

    # Test if all RPCs fail, it will raise an exception.
    data_source_client.get_runtime_envs_info.side_effect = [
        DataSourceUnavailable(),
        DataSourceUnavailable(),
        DataSourceUnavailable(),
    ]
    with pytest.raises(DataSourceUnavailable):
        result = await state_api_manager.list_runtime_envs(
            option=create_api_options(limit=1)
        )


@pytest.mark.asyncio
async def test_filter_non_existent_column(state_api_manager):
    """Test when the non existent column is given, it handles that properly.

    Related: https://github.com/ray-project/ray/issues/26811
    """
    data_source_client = state_api_manager.data_source_client
    id = b"1234"
    data_source_client.get_all_worker_info.return_value = GetAllWorkerInfoReply(
        worker_table_data=[
            generate_worker_data(id, pid=1),
            generate_worker_data(b"12345", pid=2),
        ],
        total=2,
    )
    result = await state_api_manager.list_workers(
        option=create_api_options(filters=[("exit_type", "=", "INTENDED_SYSTEM_EXIT")])
    )
    assert len(result.result) == 0


def test_type_conversion():
    # Test string
    r = _convert_filters_type([("actor_id", "=", "123")], ActorState)
    assert r[0][2] == "123"
    r = _convert_filters_type([("actor_id", "=", "abcd")], ActorState)
    assert r[0][2] == "abcd"
    r = _convert_filters_type([("actor_id", "=", "True")], ActorState)
    assert r[0][2] == "True"

    # Test boolean
    r = _convert_filters_type([("success", "=", "1")], RuntimeEnvState)
    assert r[0][2]
    r = _convert_filters_type([("success", "=", "True")], RuntimeEnvState)
    assert r[0][2]
    r = _convert_filters_type([("success", "=", "true")], RuntimeEnvState)
    assert r[0][2]
    with pytest.raises(ValueError):
        r = _convert_filters_type([("success", "=", "random_string")], RuntimeEnvState)
    r = _convert_filters_type([("success", "=", "false")], RuntimeEnvState)
    assert r[0][2] is False
    r = _convert_filters_type([("success", "=", "False")], RuntimeEnvState)
    assert r[0][2] is False
    r = _convert_filters_type([("success", "=", "0")], RuntimeEnvState)
    assert r[0][2] is False

    # Test int
    r = _convert_filters_type([("pid", "=", "0")], ObjectState)
    assert r[0][2] == 0
    r = _convert_filters_type([("pid", "=", "123")], ObjectState)
    assert r[0][2] == 123
    # Only integer can be provided.
    with pytest.raises(ValueError):
        r = _convert_filters_type([("pid", "=", "123.3")], ObjectState)
    with pytest.raises(ValueError):
        r = _convert_filters_type([("pid", "=", "abc")], ObjectState)

    # currently, there's no schema that has float column.


"""
Integration tests
"""


@pytest.mark.asyncio
async def test_state_data_source_client(ray_start_cluster):
    cluster = ray_start_cluster
    # head
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    # worker
    worker = cluster.add_node(num_cpus=2)

    GRPC_CHANNEL_OPTIONS = (
        *ray_constants.GLOBAL_GRPC_OPTIONS,
        ("grpc.max_send_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
        ("grpc.max_receive_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
    )
    gcs_channel = ray._private.utils.init_grpc_channel(
        cluster.address, GRPC_CHANNEL_OPTIONS, asynchronous=True
    )
    gcs_aio_client = GcsAioClient(address=cluster.address, nums_reconnect_retry=0)
    client = StateDataSourceClient(gcs_channel, gcs_aio_client)

    """
    Test actor
    """
    result = await client.get_all_actor_info()
    assert isinstance(result, GetAllActorInfoReply)

    """
    Test placement group
    """
    result = await client.get_all_placement_group_info()
    assert isinstance(result, GetAllPlacementGroupReply)

    """
    Test node
    """
    result = await client.get_all_node_info()
    assert isinstance(result, GetAllNodeInfoReply)

    """
    Test worker info
    """
    result = await client.get_all_worker_info()
    assert isinstance(result, GetAllWorkerInfoReply)

    """
    Test job
    """
    job_client = JobSubmissionClient(
        f"http://{ray._private.worker.global_worker.node.address_info['webui_url']}"
    )
    job_id = job_client.submit_job(  # noqa
        # Entrypoint shell command to execute
        entrypoint="ls",
    )
    result = await client.get_job_info()
    assert list(result.keys())[0] == job_id
    assert isinstance(result, dict)

    """
    Test tasks
    """
    with pytest.raises(ValueError):
        # Since we didn't register this node id, it should raise an exception.
        result = await client.get_task_info("1234")

    wait_for_condition(lambda: len(ray.nodes()) == 2)
    for node in ray.nodes():
        node_id = node["NodeID"]
        ip = node["NodeManagerAddress"]
        port = int(node["NodeManagerPort"])
        client.register_raylet_client(node_id, ip, port)
        result = await client.get_task_info(node_id)
        assert isinstance(result, GetTasksInfoReply)

    assert len(client.get_all_registered_raylet_ids()) == 2

    """
    Test objects
    """
    with pytest.raises(ValueError):
        # Since we didn't register this node id, it should raise an exception.
        result = await client.get_object_info("1234")

    wait_for_condition(lambda: len(ray.nodes()) == 2)
    for node in ray.nodes():
        node_id = node["NodeID"]
        ip = node["NodeManagerAddress"]
        port = int(node["NodeManagerPort"])
        client.register_raylet_client(node_id, ip, port)
        result = await client.get_object_info(node_id)
        assert isinstance(result, GetObjectsInfoReply)

    """
    Test runtime env
    """
    with pytest.raises(ValueError):
        # Since we didn't register this node id, it should raise an exception.
        result = await client.get_runtime_envs_info("1234")
    wait_for_condition(lambda: len(ray.nodes()) == 2)
    for node in ray.nodes():
        node_id = node["NodeID"]
        key = f"{dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX}{node_id}"

        def get_port():
            return ray.experimental.internal_kv._internal_kv_get(
                key, namespace=ray_constants.KV_NAMESPACE_DASHBOARD
            )

        wait_for_condition(lambda: get_port() is not None)
        # The second index is the gRPC port
        port = json.loads(get_port())[1]
        ip = node["NodeManagerAddress"]
        client.register_agent_client(node_id, ip, port)
        result = await client.get_runtime_envs_info(node_id)
        assert isinstance(result, GetRuntimeEnvsInfoReply)

    """
    Test logs
    """
    with pytest.raises(ValueError):
        result = await client.list_logs("1234", "*")
    with pytest.raises(ValueError):
        result = await client.stream_log("1234", "raylet.out", True, 100, 1, 5)

    wait_for_condition(lambda: len(ray.nodes()) == 2)
    # The node information should've been registered in the previous section.
    for node in ray.nodes():
        node_id = node["NodeID"]
        result = await client.list_logs(node_id, timeout=30, glob_filter="*")
        assert isinstance(result, ListLogsReply)

        stream = await client.stream_log(node_id, "raylet.out", False, 10, 1, 5)
        async for logs in stream:
            log_lines = len(logs.data.decode().split("\n"))
            assert isinstance(logs, StreamLogReply)
            assert log_lines >= 10
            assert log_lines <= 11

    """
    Test the exception is raised when the RPC error occurs.
    """
    cluster.remove_node(worker)
    # Wait until the dead node information is propagated.
    wait_for_condition(
        lambda: len(list(filter(lambda node: node["Alive"], ray.nodes()))) == 1
    )
    for node in ray.nodes():
        node_id = node["NodeID"]
        if node["Alive"]:
            continue

        # Querying to the dead node raises gRPC error, which should raise an exception.
        with pytest.raises(DataSourceUnavailable):
            await client.get_object_info(node_id)

        # Make sure unregister API works as expected.
        client.unregister_raylet_client(node_id)
        assert len(client.get_all_registered_raylet_ids()) == 1
        # Since the node_id is unregistered, the API should raise ValueError.
        with pytest.raises(ValueError):
            result = await client.get_object_info(node_id)


@pytest.mark.asyncio
async def test_state_data_source_client_limit_gcs_source(ray_start_cluster):
    cluster = ray_start_cluster
    # head
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)

    GRPC_CHANNEL_OPTIONS = (
        *ray_constants.GLOBAL_GRPC_OPTIONS,
        ("grpc.max_send_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
        ("grpc.max_receive_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
    )
    gcs_channel = ray._private.utils.init_grpc_channel(
        cluster.address, GRPC_CHANNEL_OPTIONS, asynchronous=True
    )
    gcs_aio_client = GcsAioClient(address=cluster.address, nums_reconnect_retry=0)
    client = StateDataSourceClient(gcs_channel, gcs_aio_client)

    """
    Test actor
    """

    @ray.remote
    class Actor:
        def ready(self):
            pass

    actors = [Actor.remote() for _ in range(3)]
    for actor in actors:
        ray.get(actor.ready.remote())

    result = await client.get_all_actor_info(limit=2)
    assert len(result.actor_table_data) == 2
    assert result.total == 3

    """
    Test placement group
    """
    pgs = [ray.util.placement_group(bundles=[{"CPU": 0.001}]) for _ in range(3)]  # noqa
    result = await client.get_all_placement_group_info(limit=2)
    assert len(result.placement_group_table_data) == 2
    assert result.total == 3

    """
    Test worker info
    """
    result = await client.get_all_worker_info(limit=2)
    assert len(result.worker_table_data) == 2
    # Driver + 3 workers for actors.
    assert result.total == 4


@pytest.mark.asyncio
async def test_state_data_source_client_limit_distributed_sources(ray_start_cluster):
    cluster = ray_start_cluster
    # head
    cluster.add_node(num_cpus=8)
    ray.init(address=cluster.address)

    GRPC_CHANNEL_OPTIONS = (
        *ray_constants.GLOBAL_GRPC_OPTIONS,
        ("grpc.max_send_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
        ("grpc.max_receive_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
    )
    gcs_channel = ray._private.utils.init_grpc_channel(
        cluster.address, GRPC_CHANNEL_OPTIONS, asynchronous=True
    )
    gcs_aio_client = GcsAioClient(address=cluster.address, nums_reconnect_retry=0)
    client = StateDataSourceClient(gcs_channel, gcs_aio_client)
    for node in ray.nodes():
        node_id = node["NodeID"]
        ip = node["NodeManagerAddress"]
        port = int(node["NodeManagerPort"])
        client.register_raylet_client(node_id, ip, port)

    """
    Test tasks
    """

    @ray.remote
    def long_running():
        import time

        time.sleep(300)

    @ray.remote
    def f():
        ray.get([long_running.remote() for _ in range(2)])

    # Driver: 2 * f
    # Each worker: 2 * long_running
    # -> 2 * f + 4 * long_running

    refs = [f.remote() for _ in range(2)]  # noqa

    async def verify():
        result = await client.get_task_info(node_id, limit=2)
        assert result.total == 6
        assert len(result.owned_task_info_entries) == 2
        return True

    await async_wait_for_condition_async_predicate(verify)
    for ref in refs:
        ray.cancel(ref, force=True, recursive=True)
    del refs

    """
    Test objects
    """

    @ray.remote
    def long_running_task(obj):  # noqa
        objs = [ray.put(1) for _ in range(10)]  # noqa
        import time

        time.sleep(300)

    objs = [ray.put(1) for _ in range(4)]
    refs = [long_running_task.remote(obj) for obj in objs]

    async def verify():
        result = await client.get_object_info(node_id, limit=2)
        # 4 objs (driver)
        # 4 refs (driver)
        # 4 pinned in memory for each task
        # 40 for 4 tasks * 10 objects each
        # 1 from the previous test (refs) is for some reasons not GC'ed. (driver)
        assert result.total == 53
        # Only 1 core worker stat is returned because data is truncated.
        assert len(result.core_workers_stats) == 1

        for c in result.core_workers_stats:
            # The query will be always done in the consistent ordering
            # and driver should always come first.
            assert (
                WorkerType.DESCRIPTOR.values_by_number[c.worker_type].name == "DRIVER"
            )
            assert c.objects_total == 9
            assert len(c.object_refs) == 2
        return True

    await async_wait_for_condition_async_predicate(verify)
    for ref in refs:
        ray.cancel(ref, force=True, recursive=True)
    del refs

    """
    Test runtime env
    """
    for node in ray.nodes():
        node_id = node["NodeID"]
        key = f"{dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX}{node_id}"

        def get_port():
            return ray.experimental.internal_kv._internal_kv_get(
                key, namespace=ray_constants.KV_NAMESPACE_DASHBOARD
            )

        wait_for_condition(lambda: get_port() is not None)
        # The second index is the gRPC port
        port = json.loads(get_port())[1]
        ip = node["NodeManagerAddress"]
        client.register_agent_client(node_id, ip, port)

    @ray.remote
    class Actor:
        def ready(self):
            pass

    actors = [
        Actor.options(runtime_env={"env_vars": {"index": f"{i}"}}).remote()
        for i in range(3)
    ]
    ray.get([actor.ready.remote() for actor in actors])

    result = await client.get_runtime_envs_info(node_id, limit=2)
    assert result.total == 3
    assert len(result.runtime_env_states) == 2


def is_hex(val):
    try:
        int_val = int(val, 16)
    except ValueError:
        return False
    # Should remove leading 0 because when the value is converted back
    # to hex, it is removed.
    val = val.lstrip("0")
    return f"0x{val}" == hex(int_val)


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported on Windows")
def test_cli_apis_sanity_check(ray_start_cluster):
    """Test all of CLI APIs work as expected."""
    NUM_NODES = 4
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    for _ in range(NUM_NODES - 1):
        cluster.add_node(num_cpus=2)
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
class TestListActors:
    def test_list_get_actors(self, class_ray_instance):
        @ray.remote
        class A:
            pass

        @ray.remote(num_gpus=1)
        class UnschedulableActor:
            pass

        job_id = ray.get_runtime_context().get_job_id()
        node_id = ray.get_runtime_context().get_node_id()
        a = A.remote()
        b = UnschedulableActor.remote()

        def verify():
            # Test list
            actors = list_actors(filters=[("actor_id", "=", a._actor_id.hex())])
            assert len(actors) == 1
            assert actors[0]["state"] == "ALIVE"
            assert is_hex(actors[0]["actor_id"])
            assert a._actor_id.hex() == actors[0]["actor_id"]
            assert actors[0]["job_id"] == job_id
            assert actors[0]["node_id"] == node_id

            # Test the second actor's node id is None because
            # it is not scheduled.
            actors = list_actors(filters=[("actor_id", "=", b._actor_id.hex())])
            assert actors[0]["node_id"] is None

            # Test get
            actors = list_actors(detail=True)
            for actor in actors:
                get_actor_data = get_actor(actor["actor_id"])
                assert get_actor_data is not None
                assert get_actor_data == actor

            return True

        wait_for_condition(verify)
        print(list_actors())

    def test_list_actors_namespace(self, class_ray_instance):
        """Check that list_actors returns namespaces."""

        @ray.remote
        class A:
            pass

        A.options(namespace="x").remote()
        A.options(namespace="y").remote()

        actors = list_actors()
        namespaces = Counter([actor["ray_namespace"] for actor in actors])
        assert namespaces["x"] == 1
        assert namespaces["y"] == 1

        # Check that we can filter by namespace
        x_actors = list_actors(filters=[("ray_namespace", "=", "x")])
        assert len(x_actors) == 1
        assert x_actors[0]["ray_namespace"] == "x"


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


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_get_pgs(shutdown_only):
    ray.init()
    pg = ray.util.placement_group(bundles=[{"CPU": 1}])  # noqa

    def verify():
        # Test list
        pgs = list_placement_groups()
        assert len(pgs) == 1
        assert pgs[0]["state"] == "CREATED"
        assert is_hex(pgs[0]["placement_group_id"])
        assert pg.id.hex() == pgs[0]["placement_group_id"]

        # Test get
        pgs = list_placement_groups(detail=True)
        for pg_data in pgs:
            get_pg_data = get_placement_group(pg_data["placement_group_id"])
            assert get_pg_data is not None
            assert pg_data == get_pg_data

        return True

    wait_for_condition(verify)
    print(list_placement_groups())


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_get_nodes(shutdown_only):
    ray.init()

    def verify():
        nodes = list_nodes()
        assert nodes[0]["state"] == "ALIVE"
        assert is_hex(nodes[0]["node_id"])

        # Check with legacy API
        check_nodes = ray.nodes()
        assert len(check_nodes) == len(nodes)

        sorted(check_nodes, key=lambda n: n["NodeID"])
        sorted(nodes, key=lambda n: n["node_id"])

        for check_node, node in zip(check_nodes, nodes):
            assert check_node["NodeID"] == node["node_id"]
            assert check_node["NodeName"] == node["node_name"]

        # Check the Get api
        nodes = list_nodes(detail=True)
        for node in nodes:
            get_node_data = get_node(node["node_id"])
            assert get_node_data == node

        return True

    wait_for_condition(verify)
    print(list_nodes())


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_jobs(shutdown_only):
    ray.init()
    client = JobSubmissionClient(
        f"http://{ray._private.worker.global_worker.node.address_info['webui_url']}"
    )
    job_id = client.submit_job(  # noqa
        # Entrypoint shell command to execute
        entrypoint="ls",
    )

    def verify():
        job_data = list_jobs()[0]
        print(job_data)
        job_id_from_api = job_data["job_id"]
        correct_state = job_data["status"] == "SUCCEEDED"
        correct_id = job_id == job_id_from_api
        return correct_state and correct_id

    wait_for_condition(verify)
    print(list_jobs())


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_get_workers(shutdown_only):
    ray.init()

    def verify():
        workers = list_workers()
        assert is_hex(workers[0]["worker_id"])
        # +1 to take into account of drivers.
        assert len(workers) == ray.cluster_resources()["CPU"] + 1

        # Test get worker returns the same result
        workers = list_workers(detail=True)
        for worker in workers:
            got_worker = get_worker(worker["worker_id"])
            assert got_worker == worker

        return True

    wait_for_condition(verify)
    print(list_workers())


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_cluster_events(shutdown_only):
    ray.init()

    @ray.remote(num_gpus=1)
    def f():
        pass

    f.remote()

    def verify():
        events = list_cluster_events()
        print(events)
        assert len(events) == 1
        assert (
            "Error: No available node types can fulfill " "resource request"
        ) in events[0]["message"]
        return True

    wait_for_condition(verify)
    print(list_cluster_events())

    # TODO(sang): Support get_cluster_events


def test_list_get_tasks(shutdown_only):
    ray.init(num_cpus=2)
    job_id = ray.get_runtime_context().get_job_id()
    node_id = ray.get_runtime_context().get_node_id()

    @ray.remote
    def f():
        import time

        time.sleep(30)

    @ray.remote
    def g(dep):
        import time

        time.sleep(30)

    @ray.remote(num_gpus=1)
    def impossible():
        pass

    out = [f.remote() for _ in range(2)]  # noqa
    g_out = g.remote(f.remote())  # noqa
    im = impossible.remote()  # noqa

    def verify():
        tasks = list_tasks()
        assert len(tasks) == 5
        for task in tasks:
            assert task["job_id"] == job_id
        for task in tasks:
            assert task["actor_id"] is None

        waiting_for_execution = len(
            list(
                filter(
                    lambda task: task["state"] == "SUBMITTED_TO_WORKER",
                    tasks,
                )
            )
        )
        assert waiting_for_execution == 0
        scheduled = len(
            list(
                filter(
                    lambda task: task["state"] == "PENDING_NODE_ASSIGNMENT",
                    tasks,
                )
            )
        )
        assert scheduled == 2
        waiting_for_dep = len(
            list(
                filter(
                    lambda task: task["state"] == "PENDING_ARGS_AVAIL",
                    tasks,
                )
            )
        )
        assert waiting_for_dep == 1
        running = len(
            list(
                filter(
                    lambda task: task["state"] == "RUNNING",
                    tasks,
                )
            )
        )
        assert running == 2

        # Test get tasks
        tasks = list_tasks(detail=True)
        for task in tasks:
            get_task_data = get_task(task["task_id"])
            assert get_task_data == task

        # Test node id.
        tasks = list_tasks(filters=[("state", "=", "PENDING_NODE_ASSIGNMENT")])
        for task in tasks:
            assert task["node_id"] is None

        tasks = list_tasks(filters=[("state", "=", "RUNNING")])
        for task in tasks:
            assert task["node_id"] == node_id

        tasks = list_tasks(filters=[("job_id", "=", job_id)])
        for task in tasks:
            assert task["job_id"] == job_id

        return True

    wait_for_condition(verify)
    print(list_tasks())


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


def test_list_get_task_multiple_attempt_all_failed(shutdown_only):
    ray.init(num_cpus=2)
    job_id = ray.get_runtime_context().get_job_id()
    node_id = ray.get_runtime_context().get_node_id()

    @ray.remote(retry_exceptions=True, max_retries=2)
    def f():
        raise ValueError("f is expected to failed")

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(f.remote())

    def verify(task_attempts):
        assert len(task_attempts) == 3  # 2 retries + 1 initial run
        for task_attempt in task_attempts:
            assert task_attempt["job_id"] == job_id
            assert task_attempt["state"] == "FAILED"
            assert task_attempt["node_id"] == node_id

        assert {task_attempt["attempt_number"] for task_attempt in task_attempts} == {
            0,
            1,
            2,
        }, "Attempt number should be 0,1,2"

        assert (
            len({task_attempt["task_id"] for task_attempt in task_attempts}) == 1
        ), "Same task id"
        return True

    wait_for_condition(lambda: verify(list_tasks()))

    # Test get with task id returns multiple task attempts
    task_id = list_tasks()[0]["task_id"]
    wait_for_condition(lambda: verify(get_task(task_id)))


def test_list_get_task_multiple_attempt_finished_after_retry(shutdown_only):
    ray.init(num_cpus=2)

    # Test success after retries.
    @ray.remote
    class Phaser:
        def __init__(self):
            self.i = 0

        def inc(self):
            self.i += 1
            if self.i < 3:
                raise ValueError(
                    f"First two tries are expected to fail (try={self.i})."
                )

    phaser = Phaser.remote()

    @ray.remote(retry_exceptions=True, max_retries=3)
    def f():
        ray.get(phaser.inc.remote())

    ray.get(f.remote())

    def verify(task_attempts):
        assert len(task_attempts) == 3
        for task_attempt in task_attempts[:-1]:
            assert task_attempt["state"] == "FAILED"

        task_attempts[-1]["state"] == "FINISHED"

        assert {task_attempt["attempt_number"] for task_attempt in task_attempts} == {
            0,
            1,
            2,
        }, "Attempt number should be 0,1,2"

        return True

    wait_for_condition(lambda: verify(list_tasks(filters=[("name", "=", "f")])))


def test_list_actor_tasks(shutdown_only):
    ray.init(num_cpus=2)
    job_id = ray.get_runtime_context().get_job_id()

    @ray.remote
    class Actor:
        def call(self):
            import time

            time.sleep(30)

    a = Actor.remote()
    actor_id = a._actor_id.hex()
    calls = [a.call.remote() for _ in range(10)]  # noqa

    def verify():
        tasks = list_tasks()
        for task in tasks:
            assert task["job_id"] == job_id
        for task in tasks:
            print(task)
            assert task["actor_id"] == actor_id
        # Actor.__init__: 1 finished
        # Actor.call: 1 running, 9 waiting for execution (queued).
        assert len(tasks) == 11
        assert (
            len(
                list(
                    filter(
                        lambda task: task["state"] == "SUBMITTED_TO_WORKER",
                        tasks,
                    )
                )
            )
            == 9
        )
        assert (
            len(
                list(
                    filter(
                        lambda task: task["state"] == "PENDING_NODE_ASSIGNMENT",
                        tasks,
                    )
                )
            )
            == 0
        )
        assert (
            len(
                list(
                    filter(
                        lambda task: task["state"] == "PENDING_ARGS_AVAIL",
                        tasks,
                    )
                )
            )
            == 0
        )
        assert (
            len(
                list(
                    filter(
                        lambda task: task["state"] == "RUNNING",
                        tasks,
                    )
                )
            )
            == 1
        )

        return True

    wait_for_condition(verify)
    print(list_tasks())


def test_list_get_objects(shutdown_only):
    ray.init()
    import numpy as np

    data = np.ones(50 * 1024 * 1024, dtype=np.uint8)
    plasma_obj = ray.put(data)

    @ray.remote
    def f(obj):
        print(obj)

    ray.get(f.remote(plasma_obj))

    def verify():
        obj = list_objects()[0]
        # For detailed output, the test is covered from `test_memstat.py`
        assert obj["object_id"] == plasma_obj.hex()

        obj = list_objects(detail=True)[0]
        got_objs = get_objects(plasma_obj.hex())
        assert len(got_objs) == 1
        assert obj == got_objs[0]

        return True

    wait_for_condition(verify)
    print(list_objects())


@pytest.mark.skipif(
    sys.platform == "win32", reason="Runtime env not working in Windows."
)
def test_list_runtime_envs(shutdown_only):
    ray.init(runtime_env={"pip": ["requests"]})

    @ray.remote
    class Actor:
        def ready(self):
            pass

    a = Actor.remote()  # noqa
    b = Actor.options(runtime_env={"pip": ["nonexistent_dep"]}).remote()  # noqa
    ray.get(a.ready.remote())
    with pytest.raises(ray.exceptions.RuntimeEnvSetupError):
        ray.get(b.ready.remote())

    def verify():
        result = list_runtime_envs(detail=True)
        correct_num = len(result) == 2

        failed_runtime_env = result[0]
        correct_failed_state = (
            not failed_runtime_env["success"]
            and failed_runtime_env.get("error")
            and failed_runtime_env["ref_cnt"] == "0"
        )

        successful_runtime_env = result[1]
        correct_successful_state = (
            successful_runtime_env["success"]
            and successful_runtime_env["ref_cnt"] == "2"
        )
        return correct_num and correct_failed_state and correct_successful_state

    wait_for_condition(verify)


def test_limit(shutdown_only):
    ray.init()

    @ray.remote
    class A:
        def ready(self):
            pass

    actors = [A.remote() for _ in range(4)]
    ray.get([actor.ready.remote() for actor in actors])

    output = list_actors(limit=2)
    assert len(output) == 2

    # Make sure the output is deterministic.
    assert output == list_actors(limit=2)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
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

    # Kill raylet so that list_tasks will have network error on querying raylets.
    ray._private.worker._global_node.kill_raylet()

    with pytest.raises(ConnectionError):
        list_tasks(_explain=True)


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
        with pytest.warns(None) as record:
            list_objects(_explain=True)
        assert len(record) == 0

        # Kill raylet so that list_objects will have network error on querying raylets.
        cluster.remove_node(n, allow_graceful=False)

        with pytest.warns(UserWarning):
            list_objects(raise_on_missing_output=False, _explain=True)

        # Make sure when _explain == False, warning is not printed.
        with pytest.warns(None) as record:
            list_objects(raise_on_missing_output=False, _explain=False)
        assert len(record) == 0


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
        with pytest.warns(None) as record:
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
    result = result.result
    # If the format is not yaml, it will raise an exception.
    yaml.load(
        format_list_api_output(result, schema=ActorState, format=AvailableFormat.YAML),
        Loader=yaml.FullLoader,
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
        live_workers = list_workers(filters=[("is_alive", "=", "true")])
        non_alive_workers = list_workers(filters=[("is_alive", "!=", "true")])
        assert len(live_workers) + len(non_alive_workers) == len(workers)

        live_workers = list_workers(filters=[("is_alive", "=", "1")])
        non_alive_workers = list_workers(filters=[("is_alive", "!=", "1")])
        assert len(live_workers) + len(non_alive_workers) == len(workers)

        live_workers = list_workers(filters=[("is_alive", "=", "True")])
        non_alive_workers = list_workers(filters=[("is_alive", "!=", "True")])
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

        with pytest.warns(None) as record:
            result = runner.invoke(ray_list, ["actors"])
        assert len(record) == 0


def test_detail(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    class Actor:
        def ready(self):
            pass

    a = Actor.remote()
    ray.get(a.ready.remote())

    actor_state = list_actors()[0]
    actor_state_in_detail = list_actors(detail=True)[0]

    assert set(actor_state.keys()) == ActorState.base_columns()
    assert set(actor_state_in_detail.keys()) == ActorState.columns()

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
    print(
        yaml.load(
            result.output,
            Loader=yaml.FullLoader,
        )
    )

    # When the format is given, it should respect that formatting.
    result = runner.invoke(ray_list, ["actors", "--detail", "--format=table"])
    assert result.exit_code == 0
    with pytest.raises(yaml.YAMLError):
        yaml.load(result.output, Loader=yaml.FullLoader)


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
    import time
    import threading
    import queue

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
def test_callsite_warning(callsite_enabled, monkeypatch, shutdown_only):
    # Set environment
    with monkeypatch.context() as m:
        m.setenv("RAY_record_ref_creation_sites", str(int(callsite_enabled)))
        ray.init()

        a = ray.put(1)  # noqa

        runner = CliRunner()
        wait_for_condition(lambda: len(list_objects()) > 0)

        with pytest.warns(None) as record:
            result = runner.invoke(ray_list, ["objects"])
            assert result.exit_code == 0

        if callsite_enabled:
            assert len(record) == 0
        else:
            assert len(record) == 1
            assert "RAY_record_ref_creation_sites=1" in str(record[0].message)


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
        with pytest.warns(None) as record:
            list_objects(raise_on_missing_output=False, _explain=True, timeout=3)
        assert len(record) == 1

        with pytest.warns(None) as record:
            summarize_objects(raise_on_missing_output=False, _explain=True, timeout=3)
        assert len(record) == 1

        # Verify when CLI is used, exceptions are not raised.
        with pytest.warns(None) as record:
            result = runner.invoke(ray_list, ["objects", "--timeout=3"])
        assert len(record) == 1
        assert result.exit_code == 0

        # Verify summary CLI also doesn't raise an exception.
        with pytest.warns(None) as record:
            result = runner.invoke(summary_state_cli_group, ["objects", "--timeout=3"])
        assert result.exit_code == 0
        assert len(record) == 1
        return True

    wait_for_condition(verify)


def test_raise_on_missing_output_truncation(monkeypatch, shutdown_only):
    with monkeypatch.context() as m:
        # defer for 10s for the second node.
        m.setenv(
            "RAY_MAX_LIMIT_FROM_DATA_SOURCE",
            "10",
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
            assert "Failed to retrieve all tasks from the cluster" in str(e)
            assert "(> 10)" in str(e)
        else:
            assert False

        try:
            summarize_tasks(_explain=True, timeout=3)
        except RayStateApiException as e:
            assert "Failed to retrieve all tasks from the cluster" in str(e)
            assert "(> 10)" in str(e)
        else:
            assert False

        # Verify when raise_on_missing_output=False, it prints warnings.
        with pytest.warns(None) as record:
            list_tasks(raise_on_missing_output=False, _explain=True, timeout=3)
        assert len(record) == 1

        with pytest.warns(None) as record:
            summarize_tasks(raise_on_missing_output=False, _explain=True, timeout=3)
        assert len(record) == 1

        # Verify when CLI is used, exceptions are not raised.
        with pytest.warns(None) as record:
            result = runner.invoke(ray_list, ["tasks", "--timeout=3"])
        assert len(record) == 1
        assert result.exit_code == 0

        # Verify summary CLI also doesn't raise an exception.
        with pytest.warns(None) as record:
            result = runner.invoke(summary_state_cli_group, ["tasks", "--timeout=3"])
        assert result.exit_code == 0
        assert len(record) == 1
        return True

    wait_for_condition(verify)


def test_get_id_not_found(shutdown_only):
    """Test get API CLI fails correctly when there's no corresponding id

    Related: https://github.com/ray-project/ray/issues/26808
    """
    ray.init()
    runner = CliRunner()
    result = runner.invoke(ray_get, ["actors", "1234"])
    assert result.exit_code == 0
    assert "Resource with id=1234 not found in the cluster." in result.output


def test_core_state_api_usage_tags(shutdown_only):
    from ray._private.usage.usage_lib import TagKey, get_extra_usage_tags_to_report

    ctx = ray.init()
    gcs_client = gcs_utils.GcsClient(address=ctx.address_info["gcs_address"])
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


if __name__ == "__main__":
    import os
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
