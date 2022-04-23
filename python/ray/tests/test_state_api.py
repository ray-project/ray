import sys
import pytest

from typing import List
from dataclasses import fields

from unittest.mock import MagicMock

from asyncmock import AsyncMock

import ray
import ray.ray_constants as ray_constants

from click.testing import CliRunner
from ray.cluster_utils import cluster_not_supported
from ray.core.generated.common_pb2 import (
    Address,
    WorkerType,
    TaskStatus,
    TaskInfoEntry,
    CoreWorkerStats,
    ObjectRefInfo,
)
from ray.core.generated.node_manager_pb2 import GetTasksInfoReply, GetNodeStatsReply
from ray.core.generated.gcs_pb2 import (
    ActorTableData,
    PlacementGroupTableData,
    GcsNodeInfo,
    WorkerTableData,
)
from ray.core.generated.gcs_service_pb2 import (
    GetAllActorInfoReply,
    GetAllPlacementGroupReply,
    GetAllNodeInfoReply,
    GetAllWorkerInfoReply,
)
from ray.dashboard.state_aggregator import StateAPIManager
from ray.experimental.state.api import (
    list_actors,
    list_placement_groups,
    list_nodes,
    list_jobs,
    list_workers,
    list_tasks,
    list_objects,
)
from ray.experimental.state.common import (
    ActorState,
    PlacementGroupState,
    NodeState,
    WorkerState,
    TaskState,
    ObjectState,
    ListApiOptions,
    DEFAULT_RPC_TIMEOUT,
    DEFAULT_LIMIT,
)
from ray.experimental.state.state_manager import (
    StateDataSourceClient,
    StateSourceNetworkException,
)
from ray.experimental.state.state_cli import list_state_cli_group
from ray._private.test_utils import wait_for_condition
from ray.job_submission import JobSubmissionClient

"""
Unit tests
"""


@pytest.fixture
def state_api_manager():
    data_source_client = AsyncMock(StateDataSourceClient)
    manager = StateAPIManager(data_source_client)
    yield manager


def verify_schema(state, result_dict: dict):
    state_fields_columns = set()
    for field in fields(state):
        state_fields_columns.add(field.name)

    for k in result_dict.keys():
        assert k in state_fields_columns


def generate_actor_data(id):
    return ActorTableData(
        actor_id=id,
        state=ActorTableData.ActorState.ALIVE,
        name="abc",
        pid=1234,
        class_name="class",
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


def generate_worker_data(id):
    return WorkerTableData(
        worker_address=Address(
            raylet_id=id, ip_address="127.0.0.1", port=124, worker_id=id
        ),
        is_alive=True,
        timestamp=1234,
        worker_type=WorkerType.WORKER,
    )


def generate_task_data(id, name):
    return GetTasksInfoReply(
        task_info_entries=[
            TaskInfoEntry(
                task_id=id,
                name=name,
                func_or_class_name="class",
                scheduling_state=TaskStatus.SCHEDULED,
            )
        ]
    )


def generate_object_info(obj_id):
    return CoreWorkerStats(
        pid=1234,
        worker_type=WorkerType.DRIVER,
        ip_address="1234",
        object_refs=[
            ObjectRefInfo(
                object_id=obj_id,
                call_site="",
                object_size=1,
                local_ref_count=1,
                submitted_task_ref_count=1,
                contained_in_owned=[],
                pinned_in_memory=True,
                task_status=TaskStatus.SCHEDULED,
                attempt_number=1,
            )
        ],
    )


def list_api_options(timeout: int = DEFAULT_RPC_TIMEOUT, limit: int = DEFAULT_LIMIT):
    return ListApiOptions(limit=limit, timeout=timeout)


@pytest.mark.asyncio
async def test_api_manager_list_actors(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    actor_id = b"1234"
    data_source_client.get_all_actor_info.return_value = GetAllActorInfoReply(
        actor_table_data=[generate_actor_data(actor_id), generate_actor_data(b"12345")]
    )
    result = await state_api_manager.list_actors(option=list_api_options())
    actor_data = list(result.values())[0]
    verify_schema(ActorState, actor_data)

    """
    Test limit
    """
    assert len(result) == 2
    result = await state_api_manager.list_actors(option=list_api_options(limit=1))
    assert len(result) == 1


@pytest.mark.asyncio
async def test_api_manager_list_pgs(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    id = b"1234"
    data_source_client.get_all_placement_group_info.return_value = (
        GetAllPlacementGroupReply(
            placement_group_table_data=[
                generate_pg_data(id),
                generate_pg_data(b"12345"),
            ]
        )
    )
    result = await state_api_manager.list_placement_groups(option=list_api_options())
    data = list(result.values())[0]
    verify_schema(PlacementGroupState, data)

    """
    Test limit
    """
    assert len(result) == 2
    result = await state_api_manager.list_placement_groups(
        option=list_api_options(limit=1)
    )
    assert len(result) == 1


@pytest.mark.asyncio
async def test_api_manager_list_nodes(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    id = b"1234"
    data_source_client.get_all_node_info.return_value = GetAllNodeInfoReply(
        node_info_list=[generate_node_data(id), generate_node_data(b"12345")]
    )
    result = await state_api_manager.list_nodes(option=list_api_options())
    data = list(result.values())[0]
    verify_schema(NodeState, data)

    """
    Test limit
    """
    assert len(result) == 2
    result = await state_api_manager.list_nodes(option=list_api_options(limit=1))
    assert len(result) == 1


@pytest.mark.asyncio
async def test_api_manager_list_workers(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    id = b"1234"
    data_source_client.get_all_worker_info.return_value = GetAllWorkerInfoReply(
        worker_table_data=[
            generate_worker_data(id),
            generate_worker_data(b"12345"),
        ]
    )
    result = await state_api_manager.list_workers(option=list_api_options())
    data = list(result.values())[0]
    verify_schema(WorkerState, data)

    """
    Test limit
    """
    assert len(result) == 2
    result = await state_api_manager.list_workers(option=list_api_options(limit=1))
    assert len(result) == 1


@pytest.mark.skip(
    reason=("Not passing in CI although it works locally. Will handle it later.")
)
@pytest.mark.asyncio
async def test_api_manager_list_tasks(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    data_source_client.get_all_registered_raylet_ids = MagicMock()
    data_source_client.get_all_registered_raylet_ids.return_value = ["1", "2"]

    first_task_name = "1"
    second_task_name = "2"
    data_source_client.get_task_info.side_effect = [
        generate_task_data(b"1234", first_task_name),
        generate_task_data(b"2345", second_task_name),
    ]
    result = await state_api_manager.list_tasks(option=list_api_options())
    data_source_client.get_task_info.assert_any_call("1", timeout=DEFAULT_RPC_TIMEOUT)
    data_source_client.get_task_info.assert_any_call("2", timeout=DEFAULT_RPC_TIMEOUT)
    result = list(result.values())
    assert len(result) == 2
    verify_schema(TaskState, result[0])
    verify_schema(TaskState, result[1])

    """
    Test limit
    """
    data_source_client.get_task_info.side_effect = [
        generate_task_data(b"1234", first_task_name),
        generate_task_data(b"2345", second_task_name),
    ]
    result = await state_api_manager.list_tasks(option=list_api_options(limit=1))
    assert len(result) == 1


@pytest.mark.skip(
    reason=("Not passing in CI although it works locally. Will handle it later.")
)
@pytest.mark.asyncio
async def test_api_manager_list_objects(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    obj_1_id = b"1" * 28
    obj_2_id = b"2" * 28
    data_source_client.get_all_registered_raylet_ids = MagicMock()
    data_source_client.get_all_registered_raylet_ids.return_value = ["1", "2"]

    data_source_client.get_object_info.side_effect = [
        GetNodeStatsReply(core_workers_stats=[generate_object_info(obj_1_id)]),
        GetNodeStatsReply(core_workers_stats=[generate_object_info(obj_2_id)]),
    ]
    result = await state_api_manager.list_objects(option=list_api_options())
    data_source_client.get_object_info.assert_any_call("1", timeout=DEFAULT_RPC_TIMEOUT)
    data_source_client.get_object_info.assert_any_call("2", timeout=DEFAULT_RPC_TIMEOUT)
    result = list(result.values())
    assert len(result) == 2
    verify_schema(ObjectState, result[0])
    verify_schema(ObjectState, result[1])

    """
    Test limit
    """
    data_source_client.get_object_info.side_effect = [
        GetNodeStatsReply(core_workers_stats=[generate_object_info(obj_1_id)]),
        GetNodeStatsReply(core_workers_stats=[generate_object_info(obj_2_id)]),
    ]
    result = await state_api_manager.list_objects(option=list_api_options(limit=1))
    assert len(result) == 1


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
        ("grpc.enable_http_proxy", 0),
        ("grpc.max_send_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
        ("grpc.max_receive_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
    )
    gcs_channel = ray._private.utils.init_grpc_channel(
        cluster.address, GRPC_CHANNEL_OPTIONS, asynchronous=True
    )
    client = StateDataSourceClient(gcs_channel)

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
        f"http://{ray.worker.global_worker.node.address_info['webui_url']}"
    )
    job_id = job_client.submit_job(  # noqa
        # Entrypoint shell command to execute
        entrypoint="ls",
    )
    result = client.get_job_info()
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
        assert isinstance(result, GetNodeStatsReply)

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

        # Querying to the dead node raises gRPC error, which should be
        # translated into `StateSourceNetworkException`
        with pytest.raises(StateSourceNetworkException):
            result = await client.get_object_info(node_id)

        # Make sure unregister API works as expected.
        client.unregister_raylet_client(node_id)
        assert len(client.get_all_registered_raylet_ids()) == 1
        # Since the node_id is unregistered, the API should raise ValueError.
        with pytest.raises(ValueError):
            result = await client.get_object_info(node_id)


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
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    for _ in range(3):
        cluster.add_node(num_cpus=2)
    runner = CliRunner()

    client = JobSubmissionClient(
        f"http://{ray.worker.global_worker.node.address_info['webui_url']}"
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
    job_id = client.submit_job(  # noqa
        # Entrypoint shell command to execute
        entrypoint="ls",
    )
    pg = ray.util.placement_group(bundles=[{"CPU": 1}])  # noqa

    def verify_output(resource_name, necessary_substrings: List[str]):
        result = runner.invoke(list_state_cli_group, [resource_name])
        exit_code_correct = result.exit_code == 0
        substring_matched = all(
            substr in result.output for substr in necessary_substrings
        )
        print(result.output)
        return exit_code_correct and substring_matched

    wait_for_condition(lambda: verify_output("actors", ["actor_id"]))
    wait_for_condition(lambda: verify_output("workers", ["worker_id"]))
    wait_for_condition(lambda: verify_output("nodes", ["node_id"]))
    wait_for_condition(
        lambda: verify_output("placement-groups", ["placement_group_id"])
    )
    wait_for_condition(lambda: verify_output("jobs", ["raysubmit"]))
    wait_for_condition(lambda: verify_output("tasks", ["task_id"]))
    wait_for_condition(lambda: verify_output("objects", ["object_id"]))


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_actors(shutdown_only):
    ray.init()

    @ray.remote
    class A:
        pass

    a = A.remote()  # noqa

    def verify():
        actor_data = list(list_actors().values())[0]
        correct_state = actor_data["state"] == "ALIVE"
        is_id_hex = is_hex(actor_data["actor_id"])
        correct_id = a._actor_id.hex() == actor_data["actor_id"]
        return correct_state and is_id_hex and correct_id

    wait_for_condition(verify)
    print(list_actors())


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_pgs(shutdown_only):
    ray.init()
    pg = ray.util.placement_group(bundles=[{"CPU": 1}])  # noqa

    def verify():
        pg_data = list(list_placement_groups().values())[0]
        correct_state = pg_data["state"] == "CREATED"
        is_id_hex = is_hex(pg_data["placement_group_id"])
        correct_id = pg.id.hex() == pg_data["placement_group_id"]
        return correct_state and is_id_hex and correct_id

    wait_for_condition(verify)
    print(list_placement_groups())


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_nodes(shutdown_only):
    ray.init()

    def verify():
        node_data = list(list_nodes().values())[0]
        correct_state = node_data["state"] == "ALIVE"
        is_id_hex = is_hex(node_data["node_id"])
        correct_id = ray.nodes()[0]["NodeID"] == node_data["node_id"]
        return correct_state and is_id_hex and correct_id

    wait_for_condition(verify)
    print(list_nodes())


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_jobs(shutdown_only):
    ray.init()
    client = JobSubmissionClient(
        f"http://{ray.worker.global_worker.node.address_info['webui_url']}"
    )
    job_id = client.submit_job(  # noqa
        # Entrypoint shell command to execute
        entrypoint="ls",
    )

    def verify():
        job_data = list(list_jobs().values())[0]
        job_id_from_api = list(list_jobs().keys())[0]
        correct_state = job_data["status"] == "SUCCEEDED"
        correct_id = job_id == job_id_from_api
        return correct_state and correct_id

    wait_for_condition(verify)
    print(list_jobs())


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
def test_list_workers(shutdown_only):
    ray.init()

    def verify():
        worker_data = list(list_workers().values())[0]
        is_id_hex = is_hex(worker_data["worker_id"])
        # +1 to take into account of drivers.
        correct_num_workers = len(list_workers()) == ray.cluster_resources()["CPU"] + 1
        return is_id_hex and correct_num_workers

    wait_for_condition(verify)
    print(list_workers())


def test_list_tasks(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote
    def f():
        import time

        time.sleep(30)

    @ray.remote
    def g(dep):
        import time

        time.sleep(30)

    out = [f.remote() for _ in range(2)]  # noqa
    g_out = g.remote(f.remote())  # noqa

    def verify():
        tasks = list(list_tasks().values())
        correct_num_tasks = len(tasks) == 4
        scheduled = len(
            list(filter(lambda task: task["scheduling_state"] == "SCHEDULED", tasks))
        )
        waiting_for_dep = len(
            list(
                filter(
                    lambda task: task["scheduling_state"] == "WAITING_FOR_DEPENDENCIES",
                    tasks,
                )
            )
        )

        return correct_num_tasks and scheduled == 3 and waiting_for_dep == 1

    wait_for_condition(verify)
    print(list_tasks())


def test_list_objects(shutdown_only):
    ray.init()
    import numpy as np

    data = np.ones(50 * 1024 * 1024, dtype=np.uint8)
    plasma_obj = ray.put(data)

    @ray.remote
    def f(obj):
        print(obj)

    ray.get(f.remote(plasma_obj))

    def verify():
        obj = list(list_objects().values())[0]
        # For detailed output, the test is covered from `test_memstat.py`
        return obj["object_id"] == plasma_obj.hex()

    wait_for_condition(verify)
    print(list_objects())


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
