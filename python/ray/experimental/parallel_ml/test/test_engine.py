from typing import Any

import pytest
import ray
import torch
from ray.experimental.parallel_ml.communicator.naive import NaiveCommunicator
from ray.experimental.parallel_ml.engine import Config, ExecutionEngine
from ray.experimental.parallel_ml.schedule import ExecuteSchedule
from ray.tests.conftest import *  # noqa
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
    def __init__(self):
        super().__init__()
        self.linear = nn.Linear(2, 3)

    def forward(self, x):
        return self.linear(x)


def test_engine(ray_start_4_cpus_2_gpus):
    config = Config(
        world_size=3,
        rank=1,
        input_tensor_shape=(1, 2),
        input_tensor_dtype=torch.float32,
        device_name_builder=lambda: "cpu",
        communicator_builder=lambda world_size, rank: NaiveCommunicator(
            world_size, rank
        ),
        model_builder=lambda: Model(),
        data_loader_builder=lambda: None,
    )

    input_actor = Actor.remote(3, 0, NaiveCommunicator)
    engine_actor = ray.remote(ExecutionEngine).remote(ExecuteSchedule(), config)
    output_actor = Actor.remote(3, 0, NaiveCommunicator)

    ray.get(engine_actor.start.remote())

    for _ in range(2):
        tensor = torch.rand(1, 2)
        input_actor.send.remote(tensor, 1)
        output_ref = output_actor.receive.remote(2, (1, 3), tensor.dtype)
        output = ray.get(output_ref)
        assert output.shape == (1, 3)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
