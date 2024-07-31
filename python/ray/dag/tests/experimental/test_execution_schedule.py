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

    def no_op(self, value):
        return value

    def no_op_two(self, value1, value2):
        return value1, value2

    def fwd(self, value):
        return value

    def bwd(self, value):
        return value


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_simulate_pp_2workers_1f1b(ray_start_regular, monkeypatch):
    """
    If tensor_transport is TorchTensorType.AUTO, the shared memory channel will be
    used, and the graph is valid. If tensor_transport is TorchTensorType.NCCL, the
    NCCL channel will be used, and the graph is invalid.

    [Case: TorchTensorType.NCCL]
    The first a.no_op writes to the second a.no_op via the NCCL channel. However,
    the NCCL channel only supports synchronous communication and an actor can
    only execute one task at a time, so the graph is deadlocked.
    """
    monkeypatch.setattr(ray.dag.constants, 'RAY_ADAG_ENABLE_DETECT_DEADLOCK', False)

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
    compiled_graph.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__ + "::test_valid_graph_3_actors"]))
