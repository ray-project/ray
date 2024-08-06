# coding: utf-8
import os
import sys

import pytest

import ray
import ray.cluster_utils
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.tests.conftest import *  # noqa
from ray.dag import InputNode, MultiOutputNode
from ray.dag.compiled_dag_node import DAGNodeOperationType
import torch
from typing import List
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


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
