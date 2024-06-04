# coding: utf-8
import logging
import os
import sys
import torch

import pytest

import ray
import ray.cluster_utils
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.experimental.channel.conftest import (
    Barrier,
    start_nccl_mock,
)
from ray.tests.conftest import *  # noqa
from ray.dag import InputNode

logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0, num_gpus=1)
class MockedWorker:
    def __init__(self):
        self.chan = None

    def start_mock(self):
        """
        Patch methods that require CUDA.
        """
        start_nccl_mock()

    def send(self, shape, dtype, value: int):
        return torch.ones(shape, dtype=dtype) * value

    def recv(self, tensor):
        return (tensor[0].item(), tensor.shape, tensor.dtype)


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 2,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_p2p(ray_start_cluster):
    """
    Test simple sender -> receiver pattern. Check that receiver receives
    correct results.
    """
    # Barrier name should be barrier-{sender rank}-{receiver rank}.
    barrier1 = Barrier.options(name="barrier-0-1").remote()  # noqa
    barrier2 = Barrier.options(name="barrier-1-0").remote()  # noqa

    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        # TODO(swang): Test that we are using the minimum number of
        # channels/messages when direct_return=True.
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        output_channel = compiled_dag.execute(i)
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == (i, shape, dtype)
        output_channel.end_read()

    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
