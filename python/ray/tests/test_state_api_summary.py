import time
import json
import pytest
import ray
from unittest.mock import AsyncMock
import random
import sys
from dataclasses import asdict
from concurrent.futures import ThreadPoolExecutor

from ray.util.state import (
    summarize_tasks,
    summarize_actors,
    summarize_objects,
)
from ray._private.test_utils import wait_for_condition
from ray._raylet import ActorID, TaskID, ObjectID

from ray.core.generated.common_pb2 import TaskStatus, TaskType, WorkerType
from ray.core.generated.node_manager_pb2 import GetObjectsInfoReply
from ray.core.generated.gcs_pb2 import GcsNodeInfo
from ray.tests.test_state_api import (
    generate_task_data,
    generate_task_event,
    generate_actor_data,
    generate_object_info,
)
from ray.util.state.common import (
    DEFAULT_RPC_TIMEOUT,
    SummaryApiOptions,
    Link,
    NestedTaskSummary,
    TaskSummaries,
    DRIVER_TASK_ID_PREFIX,
)
from ray.core.generated.gcs_service_pb2 import GetAllActorInfoReply, GetAllNodeInfoReply
from ray.core.generated.gcs_pb2 import ActorTableData
from click.testing import CliRunner
from ray.util.state.state_cli import summary_state_cli_group
from ray.dashboard.state_aggregator import StateAPIManager
from ray.util.state.state_manager import StateDataSourceClient


@pytest.fixture
def state_api_manager():
    data_source_client = AsyncMock(StateDataSourceClient)
    manager = StateAPIManager(
        data_source_client, thread_pool_executor=ThreadPoolExecutor()
    )
    yield manager


def create_summary_options(
    timeout: int = DEFAULT_RPC_TIMEOUT,
):
    return SummaryApiOptions(timeout=timeout)


@pytest.mark.asyncio
async def test_api_manager_summary_tasks(state_api_manager):
    data_source_client = state_api_manager.data_source_client

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


@pytest.mark.asyncio
async def test_api_manager_summary_objects(state_api_manager):
    data_source_client = state_api_manager.data_source_client
    object_ids = [ObjectID((f"{i}" * 28).encode()) for i in range(9)]
    data_source_client.get_all_node_info = AsyncMock()
    data_source_client.get_all_node_info.return_value = GetAllNodeInfoReply(
        node_info_list=[
            GcsNodeInfo(node_id=b"1" * 28, state=GcsNodeInfo.GcsNodeState.ALIVE),
            GcsNodeInfo(node_id=b"2" * 28, state=GcsNodeInfo.GcsNodeState.ALIVE),
        ]
    )
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
    assert first_summary.task_state_counts["PENDING_NODE_ASSIGNMENT"] == 2
    assert first_summary.task_state_counts["RUNNING"] == 2
    assert first_summary.task_attempt_number_counts["1"] == 3
    assert first_summary.task_attempt_number_counts["2"] == 1
    assert first_summary.ref_type_counts["PINNED_IN_MEMORY"] == 3
    assert first_summary.ref_type_counts["USED_BY_PENDING_TASK"] == 1

    second_summary = summary[second_callsite]
    assert second_summary.total_objects == 1
    assert second_summary.total_size_mb == 1.0
    assert second_summary.total_num_workers == 1
    assert second_summary.total_num_nodes == 1
    assert second_summary.task_state_counts["RUNNING"] == 1
    assert second_summary.task_attempt_number_counts["1"] == 1
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
            assert deserialized_task_arg_summary["task_state_counts"]["NIL"] == 2
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


def test_summarize_by_lineage():
    """
    Unit test for summarize by lineage.

    This test starts with an expected lineage.
    It then converts that into a single list of tasks
    It then randomizes the order of that list.
    It calls the summarize_by_lineage_function with the randomized list.
    Then asserts the final result should be the same.
    """
    expected_summary = [
        NestedTaskSummary(
            name="TuneActor",
            key="actor:tune-actor-0",
            type="ACTOR",
            timestamp=1000,
            state_counts={
                "FINISHED": 111,
                "RUNNING": 10,
            },
            link=Link("actor", "tune-actor-0"),
            children=[
                NestedTaskSummary(
                    name="TuneActor.__init__",
                    key="tune-actor-init-0",
                    type="ACTOR_CREATION_TASK",
                    timestamp=1000,
                    state_counts={
                        "FINISHED": 111,
                        "RUNNING": 10,
                    },
                    link=Link("task", "tune-actor-init-0"),
                    children=[
                        NestedTaskSummary(
                            name="TrainActor",
                            key="TrainActor",
                            type="GROUP",
                            timestamp=1100,
                            state_counts={
                                "FINISHED": 110,
                                "RUNNING": 10,
                            },
                            children=[
                                NestedTaskSummary(
                                    name="TrainActor",
                                    key=f"actor:train-actor-{i}",
                                    type="ACTOR",
                                    timestamp=1100 + i,
                                    state_counts={
                                        "FINISHED": 11,
                                        "RUNNING": 1,
                                    },
                                    link=Link("actor", f"train-actor-{i}"),
                                    children=[
                                        NestedTaskSummary(
                                            name="TrainActor.train_step_reduce",
                                            key=f"train-actor-train-step-reduce-{i}",
                                            type="ACTOR_TASK",
                                            timestamp=2200,
                                            state_counts={
                                                "RUNNING": 1,
                                            },
                                            link=Link(
                                                "task",
                                                f"train-actor-train-step-reduce-{i}",
                                            ),
                                        ),
                                        NestedTaskSummary(
                                            name="TrainActor.__init__",
                                            key=f"train-actor-init-{i}",
                                            type="ACTOR_CREATION_TASK",
                                            timestamp=1100 + i,
                                            state_counts={
                                                "FINISHED": 1,
                                            },
                                            link=Link("task", f"train-actor-init-{i}"),
                                        ),
                                        NestedTaskSummary(
                                            name="TrainActor.train_step_map",
                                            key="TrainActor.train_step_map",
                                            type="GROUP",
                                            timestamp=2100,
                                            state_counts={
                                                "FINISHED": 10,
                                            },
                                            children=[
                                                NestedTaskSummary(
                                                    name="TrainActor.train_step_map",
                                                    key=(
                                                        "train-actor-train-step-map-"
                                                        f"{i}-{j}"
                                                    ),
                                                    type="ACTOR_TASK",
                                                    timestamp=2100 + j,
                                                    state_counts={
                                                        "FINISHED": 1,
                                                    },
                                                    link=Link(
                                                        "task",
                                                        "train-actor-train-step-map-"
                                                        f"{i}-{j}",
                                                    ),
                                                )
                                                for j in range(10)
                                            ],
                                        ),
                                    ],
                                )
                                for i in range(10)
                            ],
                        )
                    ],
                )
            ],
        ),
        NestedTaskSummary(
            name="preprocess",
            key="preprocess",
            type="GROUP",
            timestamp=100,
            state_counts={
                "FINISHED": 20,
            },
            children=[
                NestedTaskSummary(
                    name="preprocess",
                    key=f"preprocess-{i}",
                    type="NORMAL_TASK",
                    timestamp=100 + i,
                    state_counts={
                        "FINISHED": 2,
                    },
                    link=Link("task", f"preprocess-{i}"),
                    children=[
                        NestedTaskSummary(
                            name="preprocess_sub_task",
                            key=f"preprocess-{i}-0",
                            type="NORMAL_TASK",
                            timestamp=200,
                            state_counts={
                                "FINISHED": 1,
                            },
                            link=Link("task", f"preprocess-{i}-0"),
                        )
                    ],
                )
                for i in range(10)
            ],
        ),
    ]

    tasks = []

    def grab_tasks_from_task_group(
        task_group: NestedTaskSummary, actor_id=None, parent_task_id=None
    ):
        if task_group.type != "ACTOR" and task_group.type != "GROUP":
            # "Virtual" groups don't have underlying tasks.
            task = {
                "name": task_group.name,
                "task_id": task_group.key,
                "parent_task_id": parent_task_id,
                "state": "RUNNING"
                if task_group.name == "TrainActor.train_step_reduce"
                else "FINISHED",
                "actor_id": actor_id,
                "creation_time_ms": task_group.timestamp,
                "func_or_class_name": task_group.name,
                "type": task_group.type,
            }
            tasks.append(task)

        actor_id_for_child = None
        parent_task_id_for_child = None

        if task_group.type == "ACTOR":
            [_, actor_id_for_child] = task_group.key.split(":")
            parent_task_id_for_child = parent_task_id
        elif task_group.type == "GROUP":
            actor_id_for_child = actor_id
            parent_task_id_for_child = parent_task_id
        else:
            parent_task_id_for_child = task_group.key

        for child in task_group.children:
            grab_tasks_from_task_group(
                child,
                actor_id=actor_id_for_child,
                parent_task_id=parent_task_id_for_child,
            )

    for group in expected_summary:
        grab_tasks_from_task_group(group, None, f"{DRIVER_TASK_ID_PREFIX}01000000")

    random.shuffle(tasks)

    summary = TaskSummaries.to_summary_by_lineage(tasks=tasks, actors=[])

    assert summary.total_tasks == 20
    assert summary.total_actor_tasks == 110
    assert summary.total_actor_scheduled == 11
    assert summary.summary == expected_summary


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
