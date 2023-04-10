from typing import Any

import pytest
import ray
import torch
from ray.experimental.parallel_ml.communicator.naive import NaiveCommunicator
from ray.experimental.parallel_ml.engine import Config, ExecutionEngine
from ray.experimental.parallel_ml.schedule import (
    ExecuteSchedule,
    InputSchedule,
    OutputSchedule,
)
from ray.tests.conftest import *  # noqa


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
        print("sending!")
        self._communicator.send(tensor, dest_rank, True)

    def receive(self, src_rank: int, shape: Any, dtype: torch.Tensor.dtype):
        print("recieving!")
        tensor = torch.ones(()).new_empty(size=shape, dtype=dtype)
        self._communicator.recv(tensor, src_rank, True)
        return tensor


def test_engine(ray_start_4_cpus_2_gpus):
    input_actor = Actor.remote(3, 0, NaiveCommunicator)
    engine_actor = ray.remote(ExecutionEngine).remote(3, 1, NaiveCommunicator)
    oupput_actor = Actor.remote(3, 0, NaiveCommunicator)
    tensor = torch.tensor([1, 2, 3])
    actor0.send.remote(tensor, 1)
    received = ray.get(actor1.receive.remote(0, tensor.shape, tensor.dtype))
    assert tensor.eq(received).all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
