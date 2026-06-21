# coding: utf-8
import os
import sys
import unittest
import pytest
import torch

import ray
from ray.dag import InputNode
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.tests.conftest import *  # noqa


@ray.remote(num_cpus=1)
class Worker:
    def send(self, val: int):
        return torch.ones(10) * val

    def recv(self, tensor: torch.Tensor):
        return (tensor[0].item(), tensor.shape, tensor.dtype)


def test_non_compiled_nccl_falls_back_to_cpu(ray_start_regular_shared):
    """
    Test that TorchTensorType(transport='nccl') in a non-compiled DAG
    falls back to CPU/shared-memory transport with a warning.
    """
    t = TorchTensorType(transport="nccl")

    with pytest.warns(UserWarning, match="Falling back to shared-memory \\(CPU\\) transport"):
        channel = t.create_channel(
            writer=None,
            reader_and_node_list=[],
        )

    from ray.experimental.channel.shared_memory_channel import CompositeChannel
    assert isinstance(channel, CompositeChannel)


def test_non_compiled_custom_communicator_falls_back_to_cpu(ray_start_regular_shared):
    """
    Test that TorchTensorType(transport=custom_comm) in a non-compiled DAG
    falls back to CPU/shared-memory transport with a warning because communicator_id is None.
    """
    from ray.experimental.channel.communicator import Communicator

    class DummyCommunicator(Communicator):
        def get_transport_name(self) -> str:
            return "nccl"

    t = TorchTensorType(transport=DummyCommunicator())

    with pytest.warns(UserWarning, match="Falling back to shared-memory \\(CPU\\) transport"):
        channel = t.create_channel(
            writer=None,
            reader_and_node_list=[],
        )

    from ray.experimental.channel.shared_memory_channel import CompositeChannel
    assert isinstance(channel, CompositeChannel)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
