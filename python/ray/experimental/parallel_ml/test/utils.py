import ray
import torch
from typing import Any

from ray.experimental.parallel_ml.communicator.naive import NaiveCommunicator
from ray.experimental.parallel_ml.communicator.torch import TorchBasedCommunicator
import pytest
from torch import nn


@pytest.fixture
def ray_start_4_cpus_2_gpus():
    address_info = ray.init(num_cpus=4, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@ray.remote
class Actor:
    def __init__(self, world_size: int, rank: int, communicator_type):
        self._communicator = communicator_type(world_size, rank)

    def send(self, tensor: torch.Tensor, dest_rank: int):
        self._communicator.send(tensor, dest_rank, True)

    def receive(self, src_rank: int, shape: Any, dtype: torch.Tensor.dtype):
        tensor = torch.ones(()).new_empty(size=shape, dtype=dtype)
        self._communicator.recv(tensor, src_rank, True)
        return tensor


class Model(nn.Module):
    def __init__(self, dim0=2, dim1=3):
        super().__init__()
        self.linear = nn.Linear(dim0, dim1)

    def forward(self, x):
        return self.linear(x)