# coding: utf-8
import logging
import os
import sys
import torch

import pytest

import ray
from ray.air._internal import torch_utils
import ray.cluster_utils
from ray.dag import InputNode
from ray.tests.conftest import *  # noqa

from ray.experimental.channel.torch_tensor_type import (
    TorchTensorType,
)


logger = logging.getLogger(__name__)

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)

USE_GPU = False


@ray.remote
class TorchTensorWorker:
    def __init__(self):
        self.device = torch_utils.get_devices()[0]

    def send(self, shape, dtype, value: int):
        return torch.ones(shape, dtype=dtype, device=self.device) * value

    def recv(self, tensor):
        # Check that tensor got loaded to the correct device.
        assert tensor.device == self.device
        return (tensor[0].item(), tensor.shape, tensor.dtype)


def test_torch_tensor_p2p(ray_start_regular_shared):
    if USE_GPU:
        assert sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 0

    actor_cls = TorchTensorWorker
    if USE_GPU:
        actor_cls = TorchTensorWorker.options(num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        dag = dag.with_type_hint(TorchTensorType(shape, dtype))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        output_channel = compiled_dag.execute(i)
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == (i, shape, dtype)
        output_channel.end_read()

    compiled_dag.teardown()

    # Passing tensors of the wrong shape will error.
    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        dag = dag.with_type_hint(TorchTensorType((20,), dtype))
        dag = receiver.recv.bind(dag)
    compiled_dag = dag.experimental_compile()
    output_channel = compiled_dag.execute(i)
    with pytest.raises(ValueError):
        output_channel.begin_read()
    compiled_dag.teardown()

    # Passing tensors of the wrong dtype will error.
    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        dag = dag.with_type_hint(TorchTensorType(shape, dtype=torch.float32))
        dag = receiver.recv.bind(dag)
    compiled_dag = dag.experimental_compile()
    output_channel = compiled_dag.execute(i)
    with pytest.raises(ValueError):
        output_channel.begin_read()
    compiled_dag.teardown()


def test_torch_tensor_as_dag_input(ray_start_regular_shared):
    if USE_GPU:
        assert sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 0

    actor_cls = TorchTensorWorker
    if USE_GPU:
        actor_cls = TorchTensorWorker.options(num_gpus=1)

    receiver = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor as input.
    with InputNode() as inp:
        torch_inp = inp.with_type_hint(TorchTensorType(shape, dtype))
        dag = receiver.recv.bind(torch_inp)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        output_channel = compiled_dag.execute(torch.ones(shape, dtype=dtype) * i)
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == (i, shape, dtype)
        output_channel.end_read()

    # Passing tensors of the wrong shape will error.
    with pytest.raises(ValueError):
        output_channel = compiled_dag.execute(torch.ones((20,), dtype=dtype) * i)

    # Passing tensors of the wrong dtype will error.
    with pytest.raises(ValueError):
        output_channel = compiled_dag.execute(
            torch.ones(shape, dtype=torch.float32) * i
        )

    compiled_dag.teardown()


def test_torch_tensor_nccl(ray_start_regular_shared):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker
    if USE_GPU:
        actor_cls = TorchTensorWorker.options(num_gpus=1)

    receiver = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor as input.
    with InputNode() as inp:
        torch_inp = inp.with_type_hint(TorchTensorType(shape, dtype, transport="nccl"))
        dag = receiver.recv.bind(torch_inp)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        output_channel = compiled_dag.execute(torch.ones(shape, dtype=dtype) * i)
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == (i, shape, dtype)
        output_channel.end_read()

    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("RAY_PYTEST_USE_GPU", 0):
        USE_GPU = True

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
