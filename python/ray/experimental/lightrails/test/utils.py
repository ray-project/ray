import logging
from typing import Any

import pytest
import ray
import torch
from torch import nn

logger = logging.getLogger(__name__)


@pytest.fixture
def ray_start_4_cpus_2_gpus():
    address_info = ray.init(num_cpus=4, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_auto():
    address_info = ray.init(address="auto")
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@ray.remote
class Actor:
    def __init__(
        self, world_size: int, rank: int, communicator_type, master_addr="localhost"
    ):
        self._communicator = communicator_type(
            world_size, rank, master_addr=master_addr
        )
        self.rank = rank
        self.master_addr = master_addr
        gpu_ids = ray.get_gpu_ids()
        logger.info(f"gpu {gpu_ids}")
        if len(gpu_ids) == 1:
            self.device = f"cuda:{gpu_ids[0]}"
        else:
            self.device = "cpu"

    def send(self, tensor: torch.Tensor, dest_rank: int):
        self._communicator.send(tensor.to(device=self.device), dest_rank, True)

    def receive(self, src_rank: int, shape: Any, dtype: torch.Tensor.dtype):
        tensor = torch.ones(()).new_empty(size=shape, dtype=dtype, device=self.device)
        logger.info(f"tensor: {tensor}, device: {self.device}")
        self._communicator.recv(tensor, src_rank, True).wait()
        return tensor.to("cpu")

    def get_master_address(self):
        if self.rank != 0:
            return self.master_addr
        else:
            import socket

            return socket.gethostname()


class Model(nn.Module):
    def __init__(self, dim0=2, dim1=3):
        super().__init__()
        self.linear = nn.Linear(dim0, dim1)

    def forward(self, x):
        return self.linear(x)
