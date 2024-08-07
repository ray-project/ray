# coding: utf-8
import os
import sys

import pytest

import ray
import ray.cluster_utils
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.tests.conftest import *  # noqa
from ray.dag import InputNode, MultiOutputNode, ClassMethodNode
from ray.dag.dag_node_operation import (
    DAGNodeOperationType,
    DAGOperationGraphNode,
    DAGNodeOperation,
)
from ray.dag.compiled_dag_node import _select_next_nodes, CompiledDAG, CompiledTask
import torch
from typing import List, Dict, Tuple
from dataclasses import dataclass, field
from collections import deque, defaultdict
from ray.actor import ActorHandle

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))


@dataclass
class PipelineConfig:
    pp_size: int
    num_micro_batches: int


@dataclass
class PipelineUnit:
    op: str
    pp_rank: int
    batch_id: int
    uid: str = field(init=False, repr=False)

    def __post_init__(self):
        self.uid = f"{self.op}_rank-{self.pp_rank}_batch-{self.batch_id}"

    def __repr__(self) -> str:
        return self.uid


def generate_1f1b_schedule(config) -> List[List[PipelineUnit]]:
    pp_size = config.pp_size
    num_micro_batches = config.num_micro_batches

    schedule = []
    for pp_rank in range(config.pp_size):
        warm_up_batches = pp_size - pp_rank
        main_1f1b_batches = num_micro_batches - warm_up_batches
        cool_down_batches = num_micro_batches - main_1f1b_batches

        rank_schedule = []
        bwd_batch_id = fwd_batch_id = 0

        for _ in range(warm_up_batches):
            rank_schedule.append(PipelineUnit("FWD", pp_rank, fwd_batch_id))
            fwd_batch_id += 1

        for _ in range(main_1f1b_batches):
            rank_schedule.append(PipelineUnit("BWD", pp_rank, bwd_batch_id))
            bwd_batch_id += 1
            rank_schedule.append(PipelineUnit("FWD", pp_rank, fwd_batch_id))
            fwd_batch_id += 1

        for _ in range(cool_down_batches):
            rank_schedule.append(PipelineUnit("BWD", pp_rank, bwd_batch_id))
            bwd_batch_id += 1
        schedule.append(rank_schedule)
    return schedule


class PipelineModel:
    def __init__(
        self,
        config: PipelineConfig,
        schedule: List[List[PipelineUnit]],
        blocks: List[ActorHandle],
        compile_dag: bool = True,
    ) -> None:
        self.config = config
        self.blocks = blocks
        self.generate_pipeline_schedules(schedule)
        self.compile_dag = compile_dag
        self.dag = self.build_dag()

    def generate_pipeline_schedules(self, schedule):
        self.id_to_unit = dict()
        self.stage_schedules = defaultdict(list)
        self.batch_schedules = defaultdict(list)

        for pp_rank, stage_schedule in enumerate(schedule):
            self.stage_schedules[pp_rank] = stage_schedule
            for unit in stage_schedule:
                self.id_to_unit[unit.uid] = unit
                self.batch_schedules[unit.batch_id].append(unit)

        for batch_id in self.batch_schedules:
            fwd_units = [
                unit for unit in self.batch_schedules[batch_id] if unit.op == "FWD"
            ]
            bwd_units = [
                unit for unit in self.batch_schedules[batch_id] if unit.op == "BWD"
            ]

            fwd_units.sort(key=lambda unit: unit.pp_rank)
            bwd_units.sort(key=lambda unit: unit.pp_rank, reverse=True)
            self.batch_schedules[batch_id] = fwd_units + bwd_units

    def build_dependency_graph(self):
        graph = defaultdict(set)
        reversed_graph = defaultdict(set)

        for schedules in [self.batch_schedules, self.stage_schedules]:
            for schedule in schedules.values():
                prev_unit = None
                for unit in schedule:
                    if prev_unit:
                        graph[prev_unit.uid].add(unit.uid)
                        reversed_graph[unit.uid].add(prev_unit.uid)
                    prev_unit = unit
        return graph, reversed_graph

    def build_dag(self):
        graph, reversed_graph = self.build_dependency_graph()
        dag_nodes = dict()  # Cache DAG Node for each unit

        first_unit = self.batch_schedules[0][0]
        queue = deque([first_unit.uid])

        with InputNode() as input_node:
            root_node = self.blocks[0].read_input.bind(input_node)

            output_nodes = []

            while queue:
                uid = queue.popleft()
                unit = self.id_to_unit[uid]
                batch_id = unit.batch_id
                batch_schedule_index = self.batch_schedules[batch_id].index(unit)

                # First forward step
                if batch_schedule_index == 0:
                    prev_dag_node = root_node
                else:
                    prev_unit = self.batch_schedules[batch_id][batch_schedule_index - 1]
                    prev_dag_node = dag_nodes[prev_unit.uid]

                block = self.blocks[unit.pp_rank]
                if unit.op == "FWD":
                    cur_dag_node = block.fwd.bind(prev_dag_node)
                else:
                    cur_dag_node = block.bwd.bind(prev_dag_node)

                # Last backward step
                if batch_schedule_index == 2 * self.config.pp_size - 1:
                    output_nodes.append(cur_dag_node)

                # ADD NCCL Channel:
                if unit.op == "FWD" and unit.pp_rank < self.config.pp_size - 1:
                    cur_dag_node.with_type_hint(
                        TorchTensorType(transport=TorchTensorType.NCCL)
                    )
                if unit.op == "BWD" and unit.pp_rank > 0:
                    cur_dag_node.with_type_hint(
                        TorchTensorType(transport=TorchTensorType.NCCL)
                    )

                dag_nodes[uid] = cur_dag_node

                # Enqueue new units
                for target_uid in graph[uid]:
                    reversed_graph[target_uid].remove(uid)
                    if not reversed_graph[target_uid]:
                        queue.append(target_uid)

            dag = MultiOutputNode(output_nodes)

            if self.compile_dag:
                dag = dag.experimental_compile()
        return dag

    def step(self, input_batches):
        return ray.get(self.dag.execute(input_batches))

    def teardown(self):
        self.dag.teardown()


@ray.remote(num_cpus=0, num_gpus=1)
class Worker:
    def __init__(self):
        pass

    def fwd(self, value):
        return value

    def bwd(self, value):
        return value

    def read_input(self, input):
        return input

    def no_op(self, value):
        return value

    def no_op_two(self, value1, value2):
        return value1, value2


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_simulate_pp_2workers_2batches_1f1b(ray_start_regular, monkeypatch):
    """
    This test simulates a simple 1F1B pipeline parallelism for training with
    2 workers and 2 batches.

    w1: fwd_b1  fwd_b2          bwd_b1          bwd_b2
    w2:         fwd_b1  bwd_b1  fwd_b2  bwd_b2

    The communication between workers is done using NCCL. The communication
    within the worker actor is done using IntraProcessChannel.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    monkeypatch.setattr(ray.dag.constants, "RAY_ADAG_ENABLE_DETECT_DEADLOCK", False)

    w1 = Worker.remote()
    w2 = Worker.remote()

    with InputNode() as inp:
        w1_input = w1.read_input.bind(inp)
        batch_1 = w1.fwd.bind(w1_input)
        batch_1.with_type_hint(TorchTensorType(transport=TorchTensorType.NCCL))
        batch_2 = w1.fwd.bind(w1_input)
        batch_2.with_type_hint(TorchTensorType(transport=TorchTensorType.NCCL))
        batch_1 = w2.fwd.bind(batch_1)
        batch_1 = w2.bwd.bind(batch_1)
        batch_1.with_type_hint(TorchTensorType(transport=TorchTensorType.NCCL))
        batch_2 = w2.fwd.bind(batch_2)
        batch_1 = w1.bwd.bind(batch_1)
        batch_2 = w2.bwd.bind(batch_2)
        batch_2.with_type_hint(TorchTensorType(transport=TorchTensorType.NCCL))
        batch_2 = w1.bwd.bind(batch_2)
        dag = MultiOutputNode(
            [
                batch_1,
                batch_2,
            ]
        )
    compiled_dag = dag.experimental_compile()

    w1_expected_schedule = [
        (0, DAGNodeOperationType.READ),
        (0, DAGNodeOperationType.COMPUTE),
        (0, DAGNodeOperationType.WRITE),
        (1, DAGNodeOperationType.READ),
        (1, DAGNodeOperationType.COMPUTE),
        (1, DAGNodeOperationType.WRITE),
        (2, DAGNodeOperationType.READ),
        (2, DAGNodeOperationType.COMPUTE),
        (2, DAGNodeOperationType.WRITE),
        (3, DAGNodeOperationType.READ),
        (3, DAGNodeOperationType.COMPUTE),
        (3, DAGNodeOperationType.WRITE),
        (4, DAGNodeOperationType.READ),
        (4, DAGNodeOperationType.COMPUTE),
        (4, DAGNodeOperationType.WRITE),
    ]
    w2_expected_schedule = [
        (0, DAGNodeOperationType.READ),
        (0, DAGNodeOperationType.COMPUTE),
        (0, DAGNodeOperationType.WRITE),
        (1, DAGNodeOperationType.READ),
        (1, DAGNodeOperationType.COMPUTE),
        (2, DAGNodeOperationType.READ),
        (1, DAGNodeOperationType.WRITE),
        (2, DAGNodeOperationType.COMPUTE),
        (2, DAGNodeOperationType.WRITE),
        (3, DAGNodeOperationType.READ),
        (3, DAGNodeOperationType.COMPUTE),
        (3, DAGNodeOperationType.WRITE),
    ]
    w1_schedule = compiled_dag.actor_to_execution_schedule[w1]
    w2_schedule = compiled_dag.actor_to_execution_schedule[w2]

    for schedule, expected_schedule in zip(
        [w1_schedule, w2_schedule], [w1_expected_schedule, w2_expected_schedule]
    ):
        assert len(schedule) == len(expected_schedule)
        for i, operation in enumerate(schedule):
            assert operation.idx == expected_schedule[i][0]
            assert operation.type == expected_schedule[i][1]

    tensor_cpu = torch.zeros(10, 10)
    ref = compiled_dag.execute(tensor_cpu)
    tensors = ray.get(ref)
    tensor_cuda = tensor_cpu.to("cuda:0")

    assert len(tensors) == 2
    for t in tensors:
        assert torch.equal(t, tensor_cuda)

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 4}], indirect=True)
def test_simulate_pp_4workers_8batches_1f1b(ray_start_regular, monkeypatch):
    """
    This test simulates a 1F1B pipeline parallelism for training with
    4 workers and 8 batches.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    monkeypatch.setattr(ray.dag.constants, "RAY_ADAG_ENABLE_DETECT_DEADLOCK", False)

    num_worker, num_batch = 4, 8

    workers = [Worker.remote() for _ in range(num_worker)]
    config = PipelineConfig(num_worker, num_batch)
    schedule = generate_1f1b_schedule(config)
    model = PipelineModel(config, schedule, workers)

    tensor_cpu = torch.zeros(10, 10)
    tensors = model.step(tensor_cpu)
    tensor_cuda = tensor_cpu.to("cuda:0")
    assert len(tensors) == num_batch
    for t in tensors:
        assert torch.equal(t, tensor_cuda)
    model.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 3}], indirect=True)
def test_three_actors_with_nccl_1(ray_start_regular):
    """
    Driver -> a.no_op -> b.no_op -> a.no_op_two -> Driver
                      |          |
                      -> c.no_op -
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    a = Worker.remote()
    b = Worker.remote()
    c = Worker.remote()

    with InputNode() as inp:
        dag = a.no_op.bind(inp)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        branch1 = b.no_op.bind(dag)
        branch1.with_type_hint(TorchTensorType(transport="nccl"))
        branch2 = c.no_op.bind(dag)
        branch2.with_type_hint(TorchTensorType(transport="nccl"))
        dag = a.no_op_two.bind(branch1, branch2)

    compiled_dag = dag.experimental_compile()

    a_expected_schedule = [
        (0, DAGNodeOperationType.READ),
        (0, DAGNodeOperationType.COMPUTE),
        (0, DAGNodeOperationType.WRITE),
        (1, DAGNodeOperationType.READ),
        (1, DAGNodeOperationType.COMPUTE),
        (1, DAGNodeOperationType.WRITE),
    ]
    b_expected_schedule = [
        (0, DAGNodeOperationType.READ),
        (0, DAGNodeOperationType.COMPUTE),
        (0, DAGNodeOperationType.WRITE),
    ]
    c_expected_schedule = [
        (0, DAGNodeOperationType.READ),
        (0, DAGNodeOperationType.COMPUTE),
        (0, DAGNodeOperationType.WRITE),
    ]
    a_schedule = compiled_dag.actor_to_execution_schedule[a]
    b_schedule = compiled_dag.actor_to_execution_schedule[b]
    c_schedule = compiled_dag.actor_to_execution_schedule[c]

    for schedule, expected_schedule in zip(
        [a_schedule, b_schedule, c_schedule],
        [a_expected_schedule, b_expected_schedule, c_expected_schedule],
    ):
        assert len(schedule) == len(expected_schedule)
        for i, operation in enumerate(schedule):
            assert operation.idx == expected_schedule[i][0]
            assert operation.type == expected_schedule[i][1]

    tensor_cpu = torch.zeros(10, 10)
    ref = compiled_dag.execute(tensor_cpu)
    tensors = ray.get(ref)
    tensor_cuda = tensor_cpu.to("cuda:0")

    assert len(tensors) == 2
    for t in tensors:
        assert torch.equal(t, tensor_cuda)

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 3}], indirect=True)
def test_three_actors_with_nccl_2(ray_start_regular, monkeypatch):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    monkeypatch.setattr(ray.dag.constants, "RAY_ADAG_ENABLE_DETECT_DEADLOCK", False)

    a = Worker.remote()
    b = Worker.remote()
    c = Worker.remote()

    with InputNode() as inp:
        branch1 = a.no_op.bind(inp)
        branch1.with_type_hint(TorchTensorType(transport="nccl"))
        branch2 = b.no_op.bind(inp)
        branch2.with_type_hint(TorchTensorType(transport="nccl"))
        branch3 = c.no_op.bind(inp)
        branch3.with_type_hint(TorchTensorType(transport="nccl"))
        dag = MultiOutputNode(
            [
                a.no_op.bind(branch3),
                b.no_op.bind(branch1),
                c.no_op.bind(branch2),
            ]
        )

    compiled_dag = dag.experimental_compile()

    a_expected_schedule = [
        (0, DAGNodeOperationType.READ),
        (0, DAGNodeOperationType.COMPUTE),
        (1, DAGNodeOperationType.READ),
        (0, DAGNodeOperationType.WRITE),
        (1, DAGNodeOperationType.COMPUTE),
        (1, DAGNodeOperationType.WRITE),
    ]
    b_expected_schedule = [
        (0, DAGNodeOperationType.READ),
        (0, DAGNodeOperationType.COMPUTE),
        (1, DAGNodeOperationType.READ),
        (0, DAGNodeOperationType.WRITE),
        (1, DAGNodeOperationType.COMPUTE),
        (1, DAGNodeOperationType.WRITE),
    ]
    c_expected_schedule = [
        (0, DAGNodeOperationType.READ),
        (0, DAGNodeOperationType.COMPUTE),
        (0, DAGNodeOperationType.WRITE),
        (1, DAGNodeOperationType.READ),
        (1, DAGNodeOperationType.COMPUTE),
        (1, DAGNodeOperationType.WRITE),
    ]

    a_schedule = compiled_dag.actor_to_execution_schedule[a]
    b_schedule = compiled_dag.actor_to_execution_schedule[b]
    c_schedule = compiled_dag.actor_to_execution_schedule[c]

    for schedule, expected_schedule in zip(
        [a_schedule, b_schedule, c_schedule],
        [a_expected_schedule, b_expected_schedule, c_expected_schedule],
    ):
        assert len(schedule) == len(expected_schedule)
        for i, operation in enumerate(schedule):
            assert operation.idx == expected_schedule[i][0]
            assert operation.type == expected_schedule[i][1]

    tensor_cpu = torch.zeros(10, 10)
    ref = compiled_dag.execute(tensor_cpu)
    tensors = ray.get(ref)
    tensor_cuda = tensor_cpu.to("cuda:0")

    assert len(tensors) == 3
    for t in tensors:
        assert torch.equal(t, tensor_cuda)

    compiled_dag.teardown()


def generate_dag_graph_nodes(local_idx, global_idx, actor_handle, requires_nccl):
    graph_nodes = {}
    for op_type in DAGNodeOperationType:
        graph_nodes[op_type] = DAGOperationGraphNode(
            DAGNodeOperation(local_idx, op_type),
            global_idx,
            actor_handle,
            requires_nccl,
        )
    return graph_nodes


class TestSelectNextNodes:
    """
    Test whether `_select_next_nodes` function selects the next nodes for
    topological sort to generate execution schedule correctly.

    global_idx: Each DAG node has a unique global index.
    local_idx: The DAG node's index in the actor's `executable_tasks` list.
    """

    def test_two_candidates_on_same_actor(self):
        """
        Simulate the case where there are two candidates on the same actor.
        The candidate with the smaller index in the `executable_tasks` list
        should be selected.

        driver -> fake_actor.op -> fake_actor.op -> driver

        In the example above, both READ operations on the fake_actor have zero
        in-degree. The operation with the smaller index in the executable_tasks
        list should be selected first; therefore, the one on the left side will
        be selected first.
        """
        fake_actor = "fake_actor"
        # The DAG node has a global index of 1, and its index in the
        # actor's `executable_tasks` list is 0.
        global_idx_1 = 1
        dag_node_1 = DAGOperationGraphNode(
            DAGNodeOperation(0, DAGNodeOperationType.READ),
            global_idx_1,
            fake_actor,
            False,
        )
        # The DAG node has a global index of 2, and its index in the
        # actor's `executable_tasks` list is 1.
        global_idx_2 = 2
        dag_node_2 = DAGOperationGraphNode(
            DAGNodeOperation(1, DAGNodeOperationType.READ),
            global_idx_2,
            fake_actor,
            False,
        )
        mock_actor_to_candidates = {
            fake_actor: [
                dag_node_1,
                dag_node_2,
            ],
        }
        next_nodes = _select_next_nodes(mock_actor_to_candidates, None)
        assert len(next_nodes) == 1
        assert next_nodes[0] == dag_node_1

    def test_only_one_nccl_write(self):
        """
        Simulate the case where there is only one candidate which is a NCCL
        WRITE operation. In this case, `_select_next_nodes` should return both
        the NCCL WRITE operation and the corresponding READ operation.

        driver -> fake_actor_1.op -> fake_actor_2.op -> driver

        In the example above, communication between fake_actor_1 and fake_actor_2
        is done using NCCL. The following test case simulates a scenario where the
        READ and COMPUTE operations on fake_actor_1 have already been added to the
        execution schedule.
        """
        fake_actor_1, global_idx_1, local_idx_1 = "fake_actor_1", 1, 0
        fake_actor_2, global_idx_2, local_idx_2 = "fake_actor_2", 2, 0
        mock_graph = {
            global_idx_1: generate_dag_graph_nodes(
                local_idx_1, global_idx_1, fake_actor_1, True
            ),
            global_idx_2: generate_dag_graph_nodes(
                local_idx_2, global_idx_2, fake_actor_2, False
            ),
        }
        del mock_graph[global_idx_1][DAGNodeOperationType.READ]
        del mock_graph[global_idx_1][DAGNodeOperationType.COMPUTE]
        mock_graph[global_idx_1][DAGNodeOperationType.WRITE].add_edge(
            mock_graph[global_idx_2][DAGNodeOperationType.READ]
        )
        mock_graph[global_idx_2][DAGNodeOperationType.READ].add_edge(
            mock_graph[global_idx_2][DAGNodeOperationType.COMPUTE]
        )
        mock_graph[global_idx_2][DAGNodeOperationType.COMPUTE].add_edge(
            mock_graph[global_idx_2][DAGNodeOperationType.WRITE]
        )
        mock_actor_to_candidates = {
            fake_actor_1: [mock_graph[global_idx_1][DAGNodeOperationType.WRITE]],
        }
        next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
        assert len(next_nodes) == 2
        assert next_nodes[0] == mock_graph[global_idx_1][DAGNodeOperationType.WRITE]
        assert next_nodes[1] == mock_graph[global_idx_2][DAGNodeOperationType.READ]

    def test_two_nccl_writes(self):
        """
        Simulate a scenario where there are two candidates that are NCCL WRITE
        operations. In this case, _select_next_nodes can choose either of the
        two NCCL WRITE operations and their corresponding READ operations.

        driver -> fake_actor_1.op -> fake_actor_2.op -> driver
               |                                     |
               -> fake_actor_2.op -> fake_actor_1.op -

        In the example above, communication between fake_actor_1 and fake_actor_2 is
        done using NCCL. The following test case simulates a scenario where the READ
        and COMPUTE operations on both the DAG nodes with smaller bind_index on
        fake_actor_1 and fake_actor_2 have already been added to the execution schedule.
        """
        fake_actor_1 = "fake_actor_1"
        global_idx_1_0, local_idx_1_0 = 1, 0
        global_idx_1_1, local_idx_1_1 = 3, 1
        fake_actor_2 = "fake_actor_2"
        global_idx_2_0, local_idx_2_0 = 2, 0
        global_idx_2_1, local_idx_2_1 = 4, 1
        mock_graph = {
            global_idx_1_0: generate_dag_graph_nodes(
                local_idx_1_0, global_idx_1_0, fake_actor_1, True
            ),
            global_idx_1_1: generate_dag_graph_nodes(
                local_idx_1_1, global_idx_1_1, fake_actor_1, False
            ),
            global_idx_2_0: generate_dag_graph_nodes(
                local_idx_2_0, global_idx_2_0, fake_actor_2, True
            ),
            global_idx_2_1: generate_dag_graph_nodes(
                local_idx_2_1, global_idx_2_1, fake_actor_2, False
            ),
        }
        del mock_graph[global_idx_1_0][DAGNodeOperationType.READ]
        del mock_graph[global_idx_1_0][DAGNodeOperationType.COMPUTE]
        del mock_graph[global_idx_2_0][DAGNodeOperationType.READ]
        del mock_graph[global_idx_2_0][DAGNodeOperationType.COMPUTE]

        mock_graph[global_idx_1_0][DAGNodeOperationType.WRITE].add_edge(
            mock_graph[global_idx_2_1][DAGNodeOperationType.READ]
        )
        mock_graph[global_idx_2_0][DAGNodeOperationType.WRITE].add_edge(
            mock_graph[global_idx_1_1][DAGNodeOperationType.READ]
        )
        mock_graph[global_idx_2_1][DAGNodeOperationType.READ].add_edge(
            mock_graph[global_idx_2_1][DAGNodeOperationType.COMPUTE]
        )
        mock_graph[global_idx_2_1][DAGNodeOperationType.COMPUTE].add_edge(
            mock_graph[global_idx_2_1][DAGNodeOperationType.WRITE]
        )
        mock_graph[global_idx_1_1][DAGNodeOperationType.READ].add_edge(
            mock_graph[global_idx_1_1][DAGNodeOperationType.COMPUTE]
        )
        mock_graph[global_idx_1_1][DAGNodeOperationType.COMPUTE].add_edge(
            mock_graph[global_idx_1_1][DAGNodeOperationType.WRITE]
        )
        mock_actor_to_candidates = {
            fake_actor_1: [mock_graph[global_idx_1_0][DAGNodeOperationType.WRITE]],
            fake_actor_2: [mock_graph[global_idx_2_0][DAGNodeOperationType.WRITE]],
        }

        next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
        assert len(next_nodes) == 2
        assert next_nodes[0] in [
            mock_graph[global_idx_1_0][DAGNodeOperationType.WRITE],
            mock_graph[global_idx_2_0][DAGNodeOperationType.WRITE],
        ]
        if next_nodes[0] == mock_graph[global_idx_1_0][DAGNodeOperationType.WRITE]:
            assert (
                next_nodes[1] == mock_graph[global_idx_2_1][DAGNodeOperationType.READ]
            )
        elif next_nodes[0] == mock_graph[global_idx_2_0][DAGNodeOperationType.WRITE]:
            assert (
                next_nodes[1] == mock_graph[global_idx_1_1][DAGNodeOperationType.READ]
            )


def mock_init(self):
    pass


class TestBuildDAGNodeOperationGraph:
    """
    Test whether `_build_dag_node_operation_graph` function adds the correct
    edges between the nodes in the operation graph base on the 3 rules mentioned
    in the doc string of `_build_dag_node_operation_graph`.
    """

    def check_edges_between_read_compute_write(
        self,
        graph: Dict[int, Dict[DAGNodeOperationType, DAGOperationGraphNode]],
        global_idx: int,
        expected_num_edges: List[Tuple[int, int]],
    ):
        """
        Check whether edges from READ to COMPUTE, and from COMPUTE to WRITE,
        belonging to the same task are added.

        Args:
            graph: The operation graph generated by `_build_dag_node_operation_graph`.
            global_idx: The global index of the task used to access the task in
                `idx_to_task`.
            expected_num_edges: A list of tuples where each tuple contains the expected
                number of in-edges and out-edges for READ, COMPUTE, and WRITE
                operations.
        """
        assert len(expected_num_edges) == 3
        assert len(graph[global_idx]) == 3
        read_node = graph[global_idx][DAGNodeOperationType.READ]
        compute_node = graph[global_idx][DAGNodeOperationType.COMPUTE]
        write_node = graph[global_idx][DAGNodeOperationType.WRITE]

        for idx, node in enumerate([read_node, compute_node, write_node]):
            assert node.in_degree == expected_num_edges[idx][0]
            assert len(node.out_edges) == expected_num_edges[idx][1]

        assert (global_idx, DAGNodeOperationType.COMPUTE) in read_node.out_edges
        assert (global_idx, DAGNodeOperationType.READ) in compute_node.in_edges
        assert (global_idx, DAGNodeOperationType.WRITE) in compute_node.out_edges
        assert (global_idx, DAGNodeOperationType.COMPUTE) in write_node.in_edges

    def check_edge_between_writer_and_reader(
        self,
        graph: Dict[int, Dict[DAGNodeOperationType, DAGOperationGraphNode]],
        writer_global_idx: int,
        reader_global_idx: int,
    ):
        """
        Check whether the edge from writer's WRITE to reader's READ operation is added.

        Args:
            graph: The operation graph generated by `_build_dag_node_operation_graph`.
            writer_global_idx: The global index of the task used to access the task
                that the writer belongs to in `idx_to_task`.
            reader_global_idx: The global index of the task used to access the task
                that the reader belongs to in `idx_to_task`.
        """
        write_node = graph[writer_global_idx][DAGNodeOperationType.WRITE]
        read_node = graph[reader_global_idx][DAGNodeOperationType.READ]

        assert (reader_global_idx, DAGNodeOperationType.READ) in write_node.out_edges
        assert (writer_global_idx, DAGNodeOperationType.WRITE) in read_node.in_edges

    def check_edge_between_compute_nodes(
        self,
        graph: Dict[int, Dict[DAGNodeOperationType, DAGOperationGraphNode]],
        global_idx_1: int,
        global_idx_2: int,
    ):
        """
        Check whether the edge from COMPUTE with `bind_index` i to COMPUTE with
            `bind_index` i+1 if they belong to the same actor.

        Args:
            graph: The operation graph generated by `_build_dag_node_operation_graph`.
            global_idx_1: The global index of the task used to access the task in
                `idx_to_task`.
            global_idx_2: The global index of the task used to access the task in
                `idx_to_task`. Note that both tasks belong to the same actor, and the
                `bind_index` of the second task is equal to the `bind_index` of the
                first task plus one.
        """
        compute_node_1 = graph[global_idx_1][DAGNodeOperationType.COMPUTE]
        compute_node_2 = graph[global_idx_2][DAGNodeOperationType.COMPUTE]

        assert (global_idx_2, DAGNodeOperationType.COMPUTE) in compute_node_1.out_edges
        assert (global_idx_1, DAGNodeOperationType.COMPUTE) in compute_node_2.in_edges

    def test_edges_between_read_compute_write(self, monkeypatch):
        """
        driver -> fake_actor.op -> driver

        This test case aims to verify whether the function correctly adds edges
        between READ/COMPUTE and COMPUTE/WRITE operations on the same actor.
        """
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_init)
        monkeypatch.setattr(MultiOutputNode, "__init__", mock_init)

        compiled_dag = CompiledDAG()
        compiled_dag.idx_to_task = {
            0: CompiledTask(0, InputNode()),
            1: CompiledTask(1, ClassMethodNode()),
            2: CompiledTask(2, MultiOutputNode()),
        }

        fake_actor = "fake_actor"
        global_idx = 1
        actor_to_operation_nodes = {
            fake_actor: [
                list(
                    generate_dag_graph_nodes(0, global_idx, fake_actor, False).values()
                )
            ]
        }
        graph = compiled_dag._build_dag_node_operation_graph(actor_to_operation_nodes)
        assert len(graph) == 1

        self.check_edges_between_read_compute_write(
            graph, global_idx, [(0, 1), (1, 1), (1, 0)]
        )

    def test_edge_between_writer_and_reader(self, monkeypatch):
        """
        driver -> fake_actor_1.op -> fake_actor_2.op -> driver

        This test case aims to verify whether the function correctly adds an edge
        from the writer's WRITE operation to the reader's READ operation.
        """
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_init)
        monkeypatch.setattr(MultiOutputNode, "__init__", mock_init)

        fake_actor_1, global_idx_1 = "fake_actor_1", 1
        fake_actor_2, global_idx_2 = "fake_actor_2", 2
        compiled_dag = CompiledDAG()
        compiled_dag.idx_to_task = {
            0: CompiledTask(0, InputNode()),
            1: CompiledTask(1, ClassMethodNode()),
            2: CompiledTask(2, ClassMethodNode()),
            3: CompiledTask(3, MultiOutputNode()),
        }
        compiled_dag.idx_to_task[1].downstream_node_idxs = {2: fake_actor_2}

        actor_to_operation_nodes = {
            fake_actor_1: [
                list(
                    generate_dag_graph_nodes(
                        0, global_idx_1, fake_actor_1, False
                    ).values()
                )
            ],
            fake_actor_2: [
                list(
                    generate_dag_graph_nodes(
                        0, global_idx_2, fake_actor_2, False
                    ).values()
                )
            ],
        }
        graph = compiled_dag._build_dag_node_operation_graph(actor_to_operation_nodes)
        assert len(graph) == 2

        self.check_edges_between_read_compute_write(
            graph, global_idx_1, [(0, 1), (1, 1), (1, 1)]
        )
        self.check_edges_between_read_compute_write(
            graph, global_idx_2, [(1, 1), (1, 1), (1, 0)]
        )
        self.check_edge_between_writer_and_reader(graph, global_idx_1, global_idx_2)

    def test_edge_between_compute_nodes(self, monkeypatch):
        """
        driver -> fake_actor.op -> fake_actor.op -> driver

        This test case aims to verify whether the function correctly adds an edge
        from the COMPUTE operation with `bind_index` i to the COMPUTE operation with
        `bind_index` i+1 if they belong to the same actor.
        """
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_init)
        monkeypatch.setattr(MultiOutputNode, "__init__", mock_init)

        fake_actor = "fake_actor"
        global_idx_1, global_idx_2 = 1, 2
        compiled_dag = CompiledDAG()
        compiled_dag.idx_to_task = {
            0: CompiledTask(0, InputNode()),
            global_idx_1: CompiledTask(global_idx_1, ClassMethodNode()),
            global_idx_2: CompiledTask(global_idx_2, ClassMethodNode()),
            3: CompiledTask(3, MultiOutputNode()),
        }
        compiled_dag.idx_to_task[global_idx_1].downstream_node_idxs = {
            global_idx_2: fake_actor
        }

        actor_to_operation_nodes = {
            fake_actor: [
                list(
                    generate_dag_graph_nodes(
                        0, global_idx_1, fake_actor, False
                    ).values()
                ),
                list(
                    generate_dag_graph_nodes(
                        1, global_idx_2, fake_actor, False
                    ).values()
                ),
            ],
        }
        graph = compiled_dag._build_dag_node_operation_graph(actor_to_operation_nodes)
        assert len(graph) == 2

        self.check_edges_between_read_compute_write(
            graph, global_idx_1, [(0, 1), (1, 2), (1, 1)]
        )
        self.check_edges_between_read_compute_write(
            graph, global_idx_2, [(1, 1), (2, 1), (1, 0)]
        )
        self.check_edge_between_writer_and_reader(graph, global_idx_1, global_idx_2)
        self.check_edge_between_compute_nodes(graph, global_idx_1, global_idx_2)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
