import time
import json
import pytest
import ray
from unittest.mock import MagicMock
import sys
from dataclasses import asdict

from ray.experimental.state.api import (
    summarize_tasks,
    summarize_actors,
    summarize_objects,
)
from ray._private.test_utils import wait_for_condition
from ray._raylet import ActorID, TaskID, ObjectID

if sys.version_info >= (3, 8, 0):
    from unittest.mock import AsyncMock
else:
    from asyncmock import AsyncMock

from ray.core.generated.common_pb2 import TaskStatus, TaskType, WorkerType
from ray.core.generated.node_manager_pb2 import GetObjectsInfoReply
from ray.tests.test_state_api import (
    generate_task_data,
    generate_task_event,
    generate_actor_data,
    generate_object_info,
)
from ray.experimental.state.common import (
    DEFAULT_RPC_TIMEOUT,
    SummaryApiOptions,
)
from ray.core.generated.gcs_service_pb2 import GetAllActorInfoReply
from ray.core.generated.gcs_pb2 import ActorTableData
from click.testing import CliRunner
from ray.experimental.state.state_cli import summary_state_cli_group
from ray.dashboard.state_aggregator import StateAPIManager
from ray.experimental.state.state_manager import StateDataSourceClient


@pytest.fixture
def state_api_manager():
    data_source_client = AsyncMock(StateDataSourceClient)
    manager = StateAPIManager(data_source_client)
    yield manager


def create_summary_options(
    timeout: int = DEFAULT_RPC_TIMEOUT,
):
    return SummaryApiOptions(timeout=timeout)


@pytest.mark.skipif(
    sys.version_info <= (3, 7, 0),
    reason=("Not passing in CI although it works locally. Will handle it later."),
)
@pytest.mark.asyncio
async def test_api_manager_summary_tasks(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    data_source_client.get_all_registered_raylet_ids = MagicMock()
    data_source_client.get_all_registered_raylet_ids.return_value = ["1", "2"]

    first_task_name = "1"
    second_task_name = "2"
    data_source_client.get_all_task_info = AsyncMock()
    ids = [TaskID((f"{i}" * 24).encode()) for i in range(5)]
    # 1: {PENDING_NODE_ASSIGNMENT:3, RUNNING:1}, 2:{PENDING_NODE_ASSIGNMENT: 1}
    data_source_client.get_all_task_info.side_effect = [
        generate_task_data(
            [
                generate_task_event(
                    id=ids[0].binary(),
                    func_or_class=first_task_name,
                    state=TaskStatus.PENDING_NODE_ASSIGNMENT,
                    type=TaskType.NORMAL_TASK,
                ),
                generate_task_event(
                    id=ids[1].binary(),
                    func_or_class=first_task_name,
                    state=TaskStatus.PENDING_NODE_ASSIGNMENT,
                    type=TaskType.NORMAL_TASK,
                ),
                generate_task_event(
                    id=ids[2].binary(),
                    func_or_class=first_task_name,
                    state=TaskStatus.PENDING_NODE_ASSIGNMENT,
                    type=TaskType.NORMAL_TASK,
                ),
                generate_task_event(
                    id=ids[3].binary(),
                    func_or_class=first_task_name,
                    state=TaskStatus.RUNNING,
                    type=TaskType.NORMAL_TASK,
                ),
                generate_task_event(
                    id=ids[4].binary(),
                    func_or_class=second_task_name,
                    state=TaskStatus.PENDING_NODE_ASSIGNMENT,
                    type=TaskType.ACTOR_TASK,
                ),
            ]
        )
    ]

    """
    Test cluster summary.
    """
    result = await state_api_manager.summarize_tasks(option=create_summary_options())
    assert "cluster" in result.result.node_id_to_summary

    data = result.result.node_id_to_summary["cluster"]
    assert data.summary[first_task_name].type == "NORMAL_TASK"
    assert data.summary[first_task_name].func_or_class_name == first_task_name
    assert data.summary[first_task_name].state_counts["PENDING_NODE_ASSIGNMENT"] == 3
    assert data.summary[first_task_name].state_counts["RUNNING"] == 1

    assert data.summary[second_task_name].type == "ACTOR_TASK"
    assert data.summary[second_task_name].func_or_class_name == second_task_name
    assert data.summary[second_task_name].state_counts["PENDING_NODE_ASSIGNMENT"] == 1

    assert data.total_tasks == 4
    assert data.total_actor_tasks == 1
    assert data.total_actor_scheduled == 0

    """
    Test if it can be correctly modified to a dictionary.
    """
    print(result.result)
    result_in_dict = asdict(result.result)
    assert json.loads(json.dumps(result_in_dict)) == result_in_dict


@pytest.mark.skipif(
    sys.version_info <= (3, 7, 0),
    reason=("Not passing in CI although it works locally. Will handle it later."),
)
@pytest.mark.asyncio
async def test_api_manager_summary_actors(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    actor_ids = [ActorID((f"{i}" * 16).encode()) for i in range(9)]
    class_a = "A"
    class_b = "B"
    data_source_client.get_all_actor_info.return_value = GetAllActorInfoReply(
        actor_table_data=[
            generate_actor_data(
                actor_ids[0].binary(),
                state=ActorTableData.ActorState.ALIVE,
                class_name=class_a,
            ),
            generate_actor_data(
                actor_ids[1].binary(),
                state=ActorTableData.ActorState.DEAD,
                class_name=class_b,
            ),
            generate_actor_data(
                actor_ids[2].binary(),
                state=ActorTableData.ActorState.PENDING_CREATION,
                class_name=class_b,
            ),
            generate_actor_data(
                actor_ids[3].binary(),
                state=ActorTableData.ActorState.DEPENDENCIES_UNREADY,
                class_name=class_b,
            ),
            generate_actor_data(
                actor_ids[4].binary(),
                state=ActorTableData.ActorState.RESTARTING,
                class_name=class_b,
            ),
            generate_actor_data(
                actor_ids[5].binary(),
                state=ActorTableData.ActorState.RESTARTING,
                class_name=class_b,
            ),
        ]
    )
    result = await state_api_manager.summarize_actors(option=create_summary_options())
    data = result.result
    assert "cluster" in result.result.node_id_to_summary
    data = result.result.node_id_to_summary["cluster"]
    assert data.total_actors == 6

    assert data.summary[class_a].class_name == class_a
    assert data.summary[class_a].state_counts["ALIVE"] == 1

    assert data.summary[class_b].class_name == class_b
    assert data.summary[class_b].state_counts["DEAD"] == 1
    assert data.summary[class_b].state_counts["DEPENDENCIES_UNREADY"] == 1
    assert data.summary[class_b].state_counts["PENDING_CREATION"] == 1
    assert data.summary[class_b].state_counts["RESTARTING"] == 2

    """
    Test if it can be correctly modified to a dictionary.
    """
    print(result.result)
    result_in_dict = asdict(result.result)
    assert json.loads(json.dumps(result_in_dict)) == result_in_dict


@pytest.mark.skipif(
    sys.version_info <= (3, 7, 0),
    reason=("Not passing in CI although it works locally. Will handle it later."),
)
@pytest.mark.asyncio
async def test_api_manager_summary_objects(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    object_ids = [ObjectID((f"{i}" * 28).encode()) for i in range(9)]
    data_source_client.get_all_registered_raylet_ids = MagicMock()
    data_source_client.get_all_registered_raylet_ids.return_value = ["1", "2"]
    first_callsite = "first.py"
    second_callsite = "second.py"

    data_source_client.get_object_info = AsyncMock()
    data_source_client.get_object_info.side_effect = [
        GetObjectsInfoReply(
            core_workers_stats=[
                generate_object_info(
                    object_ids[0].binary(),
                    size_bytes=1024**2,  # 1MB,
                    callsite=first_callsite,
                    task_state=TaskStatus.PENDING_NODE_ASSIGNMENT,
                    local_ref_count=2,
                    attempt_number=0,
                    pid=1,
                    ip="123",
                    worker_type=WorkerType.WORKER,
                    pinned_in_memory=False,
                ),
                generate_object_info(
                    object_ids[1].binary(),
                    size_bytes=1024**2,  # 1MB,
                    callsite=first_callsite,
                    task_state=TaskStatus.PENDING_NODE_ASSIGNMENT,
                    local_ref_count=2,
                    pid=2,
                    ip="123",
                    worker_type=WorkerType.WORKER,
                ),
                generate_object_info(
                    object_ids[2].binary(),
                    size_bytes=-1,
                    callsite=first_callsite,
                    task_state=TaskStatus.RUNNING,
                    local_ref_count=1,
                    attempt_number=0,
                    pid=3,
                    ip="1234",
                    worker_type=WorkerType.WORKER,
                ),
            ],
            total=3,
        ),
        GetObjectsInfoReply(
            core_workers_stats=[
                generate_object_info(
                    object_ids[3].binary(),
                    size_bytes=1024**2 * 2,  # 2MB,
                    callsite=first_callsite,
                    task_state=TaskStatus.RUNNING,
                    local_ref_count=1,
                    attempt_number=0,
                    pid=1,
                    ip="1234",
                    worker_type=WorkerType.WORKER,
                ),
                generate_object_info(
                    object_ids[4].binary(),
                    size_bytes=1024**2,  # 1MB,
                    callsite=second_callsite,
                    task_state=TaskStatus.RUNNING,
                    local_ref_count=4,
                    pid=1,
                    attempt_number=0,
                    ip="1234",
                    worker_type=WorkerType.DRIVER,
                ),
            ],
            total=2,
        ),
    ]
    result = await state_api_manager.summarize_objects(option=create_summary_options())
    assert "cluster" in result.result.node_id_to_summary
    data = result.result.node_id_to_summary["cluster"]
    assert data.total_objects == 5
    assert data.total_size_mb == 5.0
    summary = data.summary

    first_summary = summary[first_callsite]
    assert first_summary.total_objects == 4
    assert first_summary.total_size_mb == 4.0
    assert first_summary.total_num_workers == 3
    assert first_summary.total_num_nodes == 2
    assert first_summary.task_state_counts["PENDING_NODE_ASSIGNMENT"] == 1
    assert first_summary.task_state_counts["Attempt #2: PENDING_NODE_ASSIGNMENT"] == 1
    assert first_summary.task_state_counts["RUNNING"] == 2
    assert first_summary.ref_type_counts["PINNED_IN_MEMORY"] == 3
    assert first_summary.ref_type_counts["USED_BY_PENDING_TASK"] == 1

    second_summary = summary[second_callsite]
    assert second_summary.total_objects == 1
    assert second_summary.total_size_mb == 1.0
    assert second_summary.total_num_workers == 1
    assert second_summary.total_num_nodes == 1
    assert second_summary.task_state_counts["RUNNING"] == 1
    assert second_summary.ref_type_counts["PINNED_IN_MEMORY"] == 1

    """
    Test if it can be correctly modified to a dictionary.
    """
    result_in_dict = asdict(result.result)
    assert json.loads(json.dumps(result_in_dict)) == result_in_dict


def test_task_summary(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=2)

    @ray.remote
    def run_long_time_task():
        time.sleep(30)
        return True

    @ray.remote
    def task_wait_for_dep(dep):
        print(dep)

    a = task_wait_for_dep.remote(run_long_time_task.remote())  # noqa
    b = task_wait_for_dep.remote(run_long_time_task.remote())  # noqa

    def verify():
        # task_name -> states
        task_summary = summarize_tasks()
        task_summary = task_summary["cluster"]["summary"]
        assert "task_wait_for_dep" in task_summary
        assert "run_long_time_task" in task_summary
        assert (
            task_summary["task_wait_for_dep"]["state_counts"]["PENDING_ARGS_AVAIL"] == 2
        )
        assert task_summary["run_long_time_task"]["state_counts"]["RUNNING"] == 2
        assert task_summary["task_wait_for_dep"]["type"] == "NORMAL_TASK"
        return True

    wait_for_condition(verify)

    """
    Test CLI
    """
    runner = CliRunner()
    result = runner.invoke(summary_state_cli_group, ["tasks"])
    assert "task_wait_for_dep" in result.output
    assert result.exit_code == 0


def test_actor_summary(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=2)

    @ray.remote(num_gpus=1)
    class Infeasible:
        pass

    @ray.remote(num_cpus=2)
    class Actor:
        pass

    infeasible = Infeasible.remote()  # noqa
    running = [Actor.remote() for _ in range(2)]  # noqa
    pending = Actor.remote()  # noqa

    def verify():
        summary = summarize_actors()
        summary = summary["cluster"]["summary"]
        actor_summary = None
        infeasible_summary = None
        for actor_class_name, s in summary.items():
            if ".Actor" in actor_class_name:
                actor_summary = s
            elif ".Infeasible" in actor_class_name:
                infeasible_summary = s

        assert actor_summary["state_counts"]["PENDING_CREATION"] == 1
        assert actor_summary["state_counts"]["ALIVE"] == 2
        assert infeasible_summary["state_counts"]["PENDING_CREATION"] == 1
        return True

    wait_for_condition(verify)

    """
    Test CLI
    """
    runner = CliRunner()
    result = runner.invoke(summary_state_cli_group, ["actors"])
    assert "Infeasible" in result.output
    assert result.exit_code == 0


def test_object_summary(monkeypatch, ray_start_cluster):
    with monkeypatch.context() as m:
        m.setenv("RAY_record_ref_creation_sites", "1")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=4)
        ray.init(address=cluster.address)

        dep = ray.put(1)  # noqa

        @ray.remote
        def task_wait_for_dep(dep):
            time.sleep(30)

        a = [task_wait_for_dep.remote(dep) for _ in range(2)]  # noqa

        def verify():
            summary = summarize_objects()
            assert "cluster" in summary
            assert summary["cluster"]["callsite_enabled"] is True
            summary = summary["cluster"]["summary"]

            deserialized_task_arg_summary = None
            put_obj_summary = None
            return_ref_summary = None

            for k, v in summary.items():
                if "(deserialize task arg)" in k:
                    deserialized_task_arg_summary = v
                elif "(put object)" in k:
                    put_obj_summary = v
                elif "(task call)" in k:
                    return_ref_summary = v

            assert deserialized_task_arg_summary["total_objects"] == 2
            assert deserialized_task_arg_summary["total_num_workers"] == 2
            assert deserialized_task_arg_summary["total_num_nodes"] == 1
            assert deserialized_task_arg_summary["task_state_counts"]["-"] == 2
            assert (
                deserialized_task_arg_summary["ref_type_counts"]["PINNED_IN_MEMORY"]
                == 2
            )

            assert put_obj_summary["total_objects"] == 1
            assert put_obj_summary["total_num_workers"] == 1
            assert put_obj_summary["total_num_nodes"] == 1
            assert put_obj_summary["task_state_counts"]["FINISHED"] == 1
            assert put_obj_summary["ref_type_counts"]["USED_BY_PENDING_TASK"] == 1

            assert return_ref_summary["total_objects"] == 2
            assert return_ref_summary["total_num_workers"] == 1
            assert return_ref_summary["total_num_nodes"] == 1
            assert return_ref_summary["task_state_counts"]["SUBMITTED_TO_WORKER"] == 2
            assert return_ref_summary["ref_type_counts"]["LOCAL_REFERENCE"] == 2

            return True

        wait_for_condition(verify)

        """
        Test CLI
        """
        runner = CliRunner()
        result = runner.invoke(summary_state_cli_group, ["objects"])
        assert "(deserialize task arg)" in result.output
        assert result.exit_code == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
