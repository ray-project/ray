# coding: utf-8
import os
import sys

import pytest

import ray
import ray.cluster_utils
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.tests.conftest import *  # noqa
from ray.dag import InputNode, MultiOutputNode
from ray.dag.dag_node_operation import _DAGNodeOperationType
import torch
from typing import Optional
from ray.dag.compiled_dag_node import CompiledDAG

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))


@ray.remote(num_cpus=0, num_gpus=1)
class Worker:
    def __init__(self, rank: Optional[int] = None):
        self.rank = rank
        self.trace = []

    def fwd(self, value):
        self.trace.append(("FWD", self.rank))
        return value

    def bwd(self, value):
        self.trace.append(("BWD", self.rank))
        return value

    def pop_trace(self):
        trace = self.trace
        self.trace = []
        return trace

    def read_input(self, input):
        return input

    def no_op(self, value):
        return value

    def no_op_two(self, value1, value2):
        return value1, value2


def generate_1f1b_dag(
    num_workers: int, num_microbatches: int, num_lead_microbatches: int
) -> CompiledDAG:
    workers = [Worker.remote(rank) for rank in range(num_workers)]

    with ray.dag.InputNode() as inp:
        fwd_queues = [[] for _ in range(num_workers)]
        bwd_queues = [[] for _ in range(num_workers)]
        # Once a worker's counter reaches 0, it cannot execute another fwd until it
        # executes a bwd first.
        fwd_counter = [num_lead_microbatches - i for i in range(num_workers)]
        # All of the done batches.
        done = []

        # FWD on worker 0.
        input_data = workers[0].read_input.bind(inp)
        for i in range(num_microbatches):
            fwd_queues[0].append(input_data)

        while len(done) < num_microbatches:
            for i, worker in enumerate(workers):
                if fwd_counter[i] > 0 and fwd_queues[i]:
                    b = fwd_queues[i].pop(0)
                    b = worker.fwd.bind(b)
                    if i < num_workers - 1:
                        fwd_queues[i + 1].append(b)
                        # Use NCCL channel for communication between workers.
                        b.with_type_hint(
                            TorchTensorType(transport=TorchTensorType.NCCL)
                        )
                    else:
                        bwd_queues[i].append(b)
                    fwd_counter[i] -= 1
                elif bwd_queues[i]:
                    b = bwd_queues[i].pop(0)
                    b = worker.bwd.bind(b)
                    if i > 0:
                        bwd_queues[i - 1].append(b)
                        # Use NCCL channel for communication between workers.
                        b.with_type_hint(
                            TorchTensorType(transport=TorchTensorType.NCCL)
                        )
                    else:
                        done.append(b)
                    fwd_counter[i] += 1
        dag = ray.dag.MultiOutputNode(done)
    compiled_dag = dag.experimental_compile()
    return compiled_dag


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
@pytest.mark.parametrize("single_fetch", [True, False])
def test_simulate_pp_2workers_2batches_1f1b(
    ray_start_regular, single_fetch, monkeypatch
):
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
        dag = MultiOutputNode([batch_1, batch_2])
    compiled_dag = dag.experimental_compile()

    w1_expected_schedule = [
        (0, _DAGNodeOperationType.READ),
        (0, _DAGNodeOperationType.COMPUTE),
        (0, _DAGNodeOperationType.WRITE),
        (1, _DAGNodeOperationType.READ),
        (1, _DAGNodeOperationType.COMPUTE),
        (1, _DAGNodeOperationType.WRITE),
        (2, _DAGNodeOperationType.READ),
        (2, _DAGNodeOperationType.COMPUTE),
        (3, _DAGNodeOperationType.READ),
        (2, _DAGNodeOperationType.WRITE),
        (3, _DAGNodeOperationType.COMPUTE),
        (3, _DAGNodeOperationType.WRITE),
        (4, _DAGNodeOperationType.READ),
        (4, _DAGNodeOperationType.COMPUTE),
        (4, _DAGNodeOperationType.WRITE),
    ]
    w2_expected_schedule = [
        (0, _DAGNodeOperationType.READ),
        (0, _DAGNodeOperationType.COMPUTE),
        (0, _DAGNodeOperationType.WRITE),
        (1, _DAGNodeOperationType.READ),
        (1, _DAGNodeOperationType.COMPUTE),
        (1, _DAGNodeOperationType.WRITE),
        (2, _DAGNodeOperationType.READ),
        (2, _DAGNodeOperationType.COMPUTE),
        (2, _DAGNodeOperationType.WRITE),
        (3, _DAGNodeOperationType.READ),
        (3, _DAGNodeOperationType.COMPUTE),
        (3, _DAGNodeOperationType.WRITE),
    ]
    w1_schedule = compiled_dag.actor_to_execution_schedule[w1]
    w2_schedule = compiled_dag.actor_to_execution_schedule[w2]

    for schedule, expected_schedule in zip(
        [w1_schedule, w2_schedule], [w1_expected_schedule, w2_expected_schedule]
    ):
        assert len(schedule) == len(expected_schedule)
        for i, operation in enumerate(schedule):
            assert operation.local_idx == expected_schedule[i][0]
            assert operation.type == expected_schedule[i][1]

    tensor_cpu = torch.zeros(10, 10)
    tensor_cuda = tensor_cpu.to("cuda:0")
    refs = compiled_dag.execute(tensor_cpu)

    if single_fetch:
        assert len(refs) == 2
        for ref in refs:
            tensor = ray.get(ref)
            assert torch.equal(tensor, tensor_cuda)
    else:
        tensors = ray.get(refs)
        assert len(tensors) == 2
        for tensor in tensors:
            assert torch.equal(tensor, tensor_cuda)

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

    num_workers, num_microbatches, num_lead_microbatches = 4, 8, 4
    compiled_dag = generate_1f1b_dag(
        num_workers, num_microbatches, num_lead_microbatches
    )

    tensor_cpu = torch.zeros(10, 10)
    tensors = ray.get(compiled_dag.execute(tensor_cpu))
    tensor_cuda = tensor_cpu.to("cuda:0")
    assert len(tensors) == num_microbatches
    for t in tensors:
        assert torch.equal(t, tensor_cuda)
    compiled_dag.teardown()


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
        (0, _DAGNodeOperationType.READ),
        (0, _DAGNodeOperationType.COMPUTE),
        (0, _DAGNodeOperationType.WRITE),
        (1, _DAGNodeOperationType.READ),
        (1, _DAGNodeOperationType.COMPUTE),
        (1, _DAGNodeOperationType.WRITE),
    ]
    b_expected_schedule = [
        (0, _DAGNodeOperationType.READ),
        (0, _DAGNodeOperationType.COMPUTE),
        (0, _DAGNodeOperationType.WRITE),
    ]
    c_expected_schedule = [
        (0, _DAGNodeOperationType.READ),
        (0, _DAGNodeOperationType.COMPUTE),
        (0, _DAGNodeOperationType.WRITE),
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
            assert operation.local_idx == expected_schedule[i][0]
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
@pytest.mark.parametrize("single_fetch", [True, False])
def test_three_actors_with_nccl_2(ray_start_regular, single_fetch, monkeypatch):
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
        (0, _DAGNodeOperationType.READ),
        (0, _DAGNodeOperationType.COMPUTE),
        (0, _DAGNodeOperationType.WRITE),
        (1, _DAGNodeOperationType.READ),
        (1, _DAGNodeOperationType.COMPUTE),
        (1, _DAGNodeOperationType.WRITE),
    ]
    b_expected_schedule = [
        (0, _DAGNodeOperationType.READ),
        (0, _DAGNodeOperationType.COMPUTE),
        (1, _DAGNodeOperationType.READ),
        (0, _DAGNodeOperationType.WRITE),
        (1, _DAGNodeOperationType.COMPUTE),
        (1, _DAGNodeOperationType.WRITE),
    ]
    c_expected_schedule = [
        (0, _DAGNodeOperationType.READ),
        (0, _DAGNodeOperationType.COMPUTE),
        (1, _DAGNodeOperationType.READ),
        (0, _DAGNodeOperationType.WRITE),
        (1, _DAGNodeOperationType.COMPUTE),
        (1, _DAGNodeOperationType.WRITE),
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
            assert operation.local_idx == expected_schedule[i][0]
            assert operation.type == expected_schedule[i][1]

    tensor_cpu = torch.zeros(10, 10)
    tensor_cuda = tensor_cpu.to("cuda:0")
    refs = compiled_dag.execute(tensor_cpu)

    if single_fetch:
        assert len(refs) == 3
        for ref in refs:
            tensor = ray.get(ref)
            assert torch.equal(tensor, tensor_cuda)
    else:
        tensors = ray.get(refs)
        assert len(tensors) == 3
        for tensor in tensors:
            assert torch.equal(tensor, tensor_cuda)

    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
