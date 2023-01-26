import os
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

from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
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
    exclude_driver: bool = True,
):
    if not filters:
        filters = []
    return ListApiOptions(
        limit=limit,
        timeout=timeout,
        filters=filters,
        _server_timeout_multiplier=1.0,
        detail=detail,
        exclude_driver=exclude_driver,
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
        timeout=DEFAULT_RPC_TIMEOUT, job_id=None, exclude_driver=True
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