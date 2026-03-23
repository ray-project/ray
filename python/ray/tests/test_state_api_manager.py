import sys
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

import ray
from ray._common.test_utils import (
    async_wait_for_condition,
    run_string_as_driver,
)
from ray._private.state_api_test_utils import (
    create_api_options,
    get_state_api_manager,
    verify_schema,
)
from ray._raylet import NodeID
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
    GcsNodeInfo,
    PlacementGroupTableData,
    TaskEvents,
    TaskStateUpdate,
    WorkerTableData,
)
from ray.core.generated.gcs_service_pb2 import (
    GcsStatus,
    GetAllActorInfoReply,
    GetAllPlacementGroupReply,
    GetAllWorkerInfoReply,
    GetTaskEventsReply,
)
from ray.core.generated.node_manager_pb2 import GetObjectsInfoReply
from ray.core.generated.runtime_env_agent_pb2 import GetRuntimeEnvsInfoReply
from ray.core.generated.runtime_env_common_pb2 import (
    RuntimeEnvState as RuntimeEnvStateProto,
)
from ray.dashboard.state_aggregator import (
    GCS_QUERY_FAILURE_WARNING,
    NODE_QUERY_FAILURE_WARNING,
    StateAPIManager,
)
from ray.runtime_env import RuntimeEnv
from ray.util.state.common import (
    DEFAULT_RPC_TIMEOUT,
    ActorState,
    NodeState,
    ObjectState,
    PlacementGroupState,
    RuntimeEnvState,
    SummaryApiOptions,
    TaskState,
    WorkerState,
)
from ray.util.state.exception import DataSourceUnavailable
from ray.util.state.state_manager import StateDataSourceClient


@pytest.fixture
def state_api_manager():
    data_source_client = AsyncMock(StateDataSourceClient)
    manager = StateAPIManager(
        data_source_client, thread_pool_executor=ThreadPoolExecutor()
    )
    yield manager


@pytest_asyncio.fixture
async def state_api_manager_e2e(ray_start_with_dashboard):
    address_info = ray_start_with_dashboard
    gcs_address = address_info["gcs_address"]
    manager = get_state_api_manager(gcs_address)
    yield manager


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


def generate_worker_data(
    id,
    pid=1234,
    worker_launch_time_ms=1,
    worker_launched_time_ms=2,
    start_time_ms=3,
    end_time_ms=4,
):
    return WorkerTableData(
        worker_address=Address(
            node_id=id, ip_address="127.0.0.1", port=124, worker_id=id
        ),
        is_alive=True,
        timestamp=1234,
        worker_type=WorkerType.WORKER,
        pid=pid,
        exit_type=None,
        worker_launch_time_ms=worker_launch_time_ms,
        worker_launched_time_ms=worker_launched_time_ms,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
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


def generate_failure_test_data():
    return GetTaskEventsReply(
        status=GcsStatus(code=34, message="Unknown filter predicate"),
        events_by_task=[],
        num_status_task_events_dropped=0,
        num_profile_task_events_dropped=0,
        num_total_stored=0,
        num_filtered_on_gcs=0,
        num_truncated=0,
    )


def generate_early_return_task_data():
    return GetTaskEventsReply(
        num_profile_task_events_dropped=0,
        num_status_task_events_dropped=0,
        num_total_stored=0,
        num_filtered_on_gcs=0,
        num_truncated=0,
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
async def test_api_manager_e2e_list_actors(state_api_manager_e2e):
    @ray.remote
    class Actor:
        pass

    a = Actor.remote()
    script = """
import ray

ray.init("auto")

@ray.remote
class Actor:
    pass

    def ready(self):
        pass

b = Actor.remote()
ray.get(b.ready.remote())
del b
    """

    run_string_as_driver(script)

    async def verify():
        result = await state_api_manager_e2e.list_actors(option=create_api_options())
        print(result)
        assert result.total == 2
        assert result.num_after_truncation == 2
        return True

    await async_wait_for_condition(verify)

    async def verify():
        # Test actor id filtering on source
        result = await state_api_manager_e2e.list_actors(
            option=create_api_options(filters=[("actor_id", "=", a._actor_id.hex())])
        )
        print(result)
        assert result.num_after_truncation == 2
        assert len(result.result) == 1
        return True

    await async_wait_for_condition(verify)

    async def verify():
        # Test state filtering on source
        result = await state_api_manager_e2e.list_actors(
            option=create_api_options(filters=[("state", "=", "ALIVE")])
        )
        assert result.num_after_truncation == 2
        assert len(result.result) == 1
        return True

    await async_wait_for_condition(verify)

    async def verify():
        # Test job filtering on source
        cur_job_id = ray.get_runtime_context().get_job_id()
        result = await state_api_manager_e2e.list_actors(
            option=create_api_options(filters=[("job_id", "=", cur_job_id)])
        )
        assert result.num_after_truncation == 2
        assert len(result.result) == 1
        return True

    await async_wait_for_condition(verify)

    async def verify():
        with pytest.raises(ValueError):
            await state_api_manager_e2e.list_actors(
                option=create_api_options(filters=[("state", "=", "DEEEED")])
            )

        return True

    await async_wait_for_condition(verify)


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
async def test_api_manager_list_nodes(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    id = b"1234"
    node_1 = generate_node_data(id)
    node_2 = generate_node_data(b"12345")
    data_source_client.get_all_node_info.return_value = (
        {
            node_1.node_id: node_1,
            node_2.node_id: node_2,
        },
        0,
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
    data_source_client.get_all_node_info.return_value = (
        {
            node_1.node_id: node_1,
        },
        1,
    )
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
    data_source_client.get_all_node_info.return_value = (
        {
            node_1.node_id: node_1,
        },
        1,
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
        timeout=DEFAULT_RPC_TIMEOUT, filters=[], exclude_driver=True
    )
    data = result.result
    data = data
    assert len(data) == 2
    assert result.total == 2
    print(data)
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

    """
    Test failure reply
    """
    data_source_client.get_all_task_info.side_effect = [generate_failure_test_data()]
    result = await state_api_manager.list_tasks(option=create_api_options())
    assert len(result.result) == 0
    assert result.total == 0
    assert result.num_filtered == 0
    assert result.num_after_truncation == 0
    assert len(result.warnings) > 0

    """
    Test early reply
    """
    data_source_client.get_all_task_info.side_effect = [
        generate_early_return_task_data()
    ]
    result = await state_api_manager.list_tasks(option=create_api_options())
    assert len(result.result) == 0
    assert result.total == 0
    assert result.num_filtered == 0
    assert result.num_after_truncation == 0
    assert result.warnings is None


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
        state_ts_ns={
            TaskStatus.PENDING_ARGS_AVAIL: current,
            TaskStatus.SUBMITTED_TO_WORKER: current + second,
            TaskStatus.RUNNING: current + (2 * second),
            TaskStatus.FINISHED: current + (3 * second),
        },
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
        state_ts_ns={
            TaskStatus.PENDING_ARGS_AVAIL: current,
            TaskStatus.SUBMITTED_TO_WORKER: current + second,
            TaskStatus.RUNNING: current + (2 * second),
        },
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
        state_ts_ns={
            TaskStatus.PENDING_ARGS_AVAIL: current,
            TaskStatus.SUBMITTED_TO_WORKER: current + second,
        },
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


@pytest.mark.asyncio
async def test_api_manager_list_objects(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    obj_1_id = b"1" * 28
    obj_2_id = b"2" * 28
    data_source_client.get_all_node_info = AsyncMock()
    data_source_client.get_all_node_info.return_value = (
        {
            NodeID.from_binary(b"1" * 28): GcsNodeInfo(
                node_id=b"1" * 28,
                state=GcsNodeInfo.GcsNodeState.ALIVE,
                node_manager_address="192.168.1.1",
                node_manager_port=10001,
            ),
            NodeID.from_binary(b"2" * 28): GcsNodeInfo(
                node_id=b"2" * 28,
                state=GcsNodeInfo.GcsNodeState.ALIVE,
                node_manager_address="192.168.1.2",
                node_manager_port=10002,
            ),
        },
        0,
    )

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
        "192.168.1.1", 10001, timeout=DEFAULT_RPC_TIMEOUT
    )
    data_source_client.get_object_info.assert_any_await(
        "192.168.1.2", 10002, timeout=DEFAULT_RPC_TIMEOUT
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


@pytest.mark.asyncio
async def test_api_manager_list_runtime_envs(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    data_source_client.get_all_node_info = AsyncMock()
    data_source_client.get_all_node_info.return_value = (
        {
            NodeID.from_binary(b"1" * 28): GcsNodeInfo(
                node_id=b"1" * 28,
                node_manager_address="192.168.1.1",
                state=GcsNodeInfo.GcsNodeState.ALIVE,
                runtime_env_agent_port=10000,
            ),
            NodeID.from_binary(b"2" * 28): GcsNodeInfo(
                node_id=b"2" * 28,
                node_manager_address="192.168.1.2",
                state=GcsNodeInfo.GcsNodeState.ALIVE,
                runtime_env_agent_port=10001,
            ),
            NodeID.from_binary(b"3" * 28): GcsNodeInfo(
                node_id=b"3" * 28,
                node_manager_address="192.168.1.3",
                state=GcsNodeInfo.GcsNodeState.ALIVE,
                runtime_env_agent_port=10002,
            ),
        },
        0,
    )

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
        "192.168.1.1", 10000, timeout=DEFAULT_RPC_TIMEOUT
    )
    data_source_client.get_runtime_envs_info.assert_any_await(
        "192.168.1.2", 10001, timeout=DEFAULT_RPC_TIMEOUT
    )

    data_source_client.get_runtime_envs_info.assert_any_await(
        "192.168.1.3", 10002, timeout=DEFAULT_RPC_TIMEOUT
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
