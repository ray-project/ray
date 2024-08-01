# coding: utf-8
import os
import sys

import pytest

import ray
import ray.cluster_utils
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.experimental.channel.conftest import start_nccl_mock
from ray.tests.conftest import *  # noqa
from ray.dag import InputNode, MultiOutputNode
from ray.dag.compiled_dag_node import DAGNodeOperationType

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)


@ray.remote(num_cpus=0, num_gpus=1)
class MockedWorker:
    def __init__(self):
        pass

    def start_mock(self):
        """
        Patch methods that require CUDA.
        """
        start_nccl_mock()

    def fwd(self, value):
        return value

    def bwd(self, value):
        return value

    def no_op(self, value):
        return value

    def no_op_two(self, value1, value2):
        return value1, value2


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_simulate_pp_2workers_1f1b(ray_start_regular, monkeypatch):
    """
    This test simulates a simple 1F1B pipeline parallelism for training with
    2 workers and 2 batches.

    w1: fwd_b1  fwd_b2          bwd_b1          bwd_b2
    w2:         fwd_b1  bwd_b1  fwd_b2  bwd_b2

    The communication between workers is done using NCCL. The communication
    within the worker actor is done using IntraProcessChannel.
    """
    monkeypatch.setattr(ray.dag.constants, "RAY_ADAG_ENABLE_DETECT_DEADLOCK", False)

    w1 = MockedWorker.remote()
    w2 = MockedWorker.remote()

    ray.get([w1.start_mock.remote(), w2.start_mock.remote()])

    with InputNode() as inp:
        batch_1 = w1.fwd.bind(inp)
        batch_1.with_type_hint(TorchTensorType(transport=TorchTensorType.NCCL))
        batch_2 = w1.fwd.bind(inp)
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
    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 3}], indirect=True)
def test_three_actors_with_nccl_1(ray_start_regular):
    """
    Driver -> a.no_op -> b.no_op -> a.no_op_two -> Driver
                      |          |
                      -> c.no_op -
    """
    a = MockedWorker.remote()
    b = MockedWorker.remote()
    c = MockedWorker.remote()

    ray.get([a.start_mock.remote(), b.start_mock.remote(), c.start_mock.remote()])

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

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 3}], indirect=True)
def test_three_actors_with_nccl_2(ray_start_regular, monkeypatch):
    monkeypatch.setattr(ray.dag.constants, "RAY_ADAG_ENABLE_DETECT_DEADLOCK", False)

    a = MockedWorker.remote()
    b = MockedWorker.remote()
    c = MockedWorker.remote()

    ray.get([a.start_mock.remote(), b.start_mock.remote(), c.start_mock.remote()])

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

    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
