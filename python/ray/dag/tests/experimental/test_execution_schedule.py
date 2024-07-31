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
    compiled_graph = dag.experimental_compile()

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
    w1_schedule = compiled_graph.actor_to_execution_schedule[w1]
    w2_schedule = compiled_graph.actor_to_execution_schedule[w2]

    for schedule, expected_schedule in zip(
        [w1_schedule, w2_schedule], [w1_expected_schedule, w2_expected_schedule]
    ):
        assert len(schedule) == len(expected_schedule)
        for i, operation in enumerate(schedule):
            assert operation.idx == expected_schedule[i][0]
            assert operation.type == expected_schedule[i][1]
    compiled_graph.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
