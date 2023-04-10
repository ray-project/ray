from typing import Any

import pytest
import ray
import torch
from ray.experimental.parallel_ml.communicator import (
    NaiveCommunicator,
    TorchBasedCommunicator,
)
from ray.experimental.parallel_ml.engine import Config, ExecutionEngine
from ray.experimental.parallel_ml.schedule import ExecuteSchedule
from ray.experimental.parallel_ml.test.utils import (
    Actor,
    Model,
    ray_start_4_cpus_2_gpus,
    ray_start_auto,
)
from ray.tests.conftest import *  # noqa


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
    engine_actor = ray.remote(ExecutionEngine).remote(ExecuteSchedule(0, 2), config)
    output_actor = Actor.remote(3, 2, NaiveCommunicator)

    ray.get(engine_actor.start.remote())

    for _ in range(2):
        tensor = torch.rand(1, 2)
        input_actor.send.remote(tensor, 1)
        output_ref = output_actor.receive.remote(1, (1, 3), tensor.dtype)
        output = ray.get(output_ref)
        assert output.shape == (1, 3)

    engine_actor.stop.remote()


def test_pipelines(ray_start_4_cpus_2_gpus):
    config1 = Config(
        world_size=4,
        rank=1,
        input_tensor_shape=(1, 2),
        input_tensor_dtype=torch.float32,
        device_name_builder=lambda: "cpu",
        communicator_builder=lambda world_size, rank: NaiveCommunicator(
            world_size, rank
        ),
        model_builder=lambda: Model(2, 3),
        data_loader_builder=lambda: None,
    )

    config2 = Config(
        world_size=4,
        rank=2,
        input_tensor_shape=(1, 3),
        input_tensor_dtype=torch.float32,
        device_name_builder=lambda: "cpu",
        communicator_builder=lambda world_size, rank: NaiveCommunicator(
            world_size, rank
        ),
        model_builder=lambda: Model(3, 4),
        data_loader_builder=lambda: None,
    )

    input_actor = Actor.remote(4, 0, NaiveCommunicator)
    engine_actor1 = ray.remote(ExecutionEngine).remote(ExecuteSchedule(0, 2), config1)
    engine_actor2 = ray.remote(ExecutionEngine).remote(ExecuteSchedule(1, 3), config2)
    output_actor = Actor.remote(4, 3, NaiveCommunicator)

    ray.get(engine_actor1.start.remote())
    ray.get(engine_actor2.start.remote())

    for _ in range(2):
        tensor = torch.rand(1, 2)
        input_actor.send.remote(tensor, 1)
        output_ref = output_actor.receive.remote(2, (1, 4), tensor.dtype)
        output = ray.get(output_ref)
        assert output.shape == (1, 4)

    engine_actor1.stop.remote()
    engine_actor2.stop.remote()


@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires at least 2 GPUs")
def test_gpu_pipelines(ray_start_auto):
    input_actor = Actor.options(num_gpus=1).remote(4, 0, TorchBasedCommunicator)
    address = ray.get(input_actor.get_master_address.remote())

    config1 = Config(
        world_size=4,
        rank=1,
        input_tensor_shape=(1, 2),
        input_tensor_dtype=torch.float32,
        device_name_builder=lambda: "cuda:0",
        communicator_builder=lambda world_size, rank: TorchBasedCommunicator(
            world_size, rank, master_addr=address
        ),
        model_builder=lambda: Model(2, 3),
        data_loader_builder=lambda: None,
    )

    config2 = Config(
        world_size=4,
        rank=2,
        input_tensor_shape=(1, 3),
        input_tensor_dtype=torch.float32,
        device_name_builder=lambda: "cuda:0",
        communicator_builder=lambda world_size, rank: TorchBasedCommunicator(
            world_size, rank, master_addr=address
        ),
        model_builder=lambda: Model(3, 4),
        data_loader_builder=lambda: None,
    )

    engine_actor1 = (
        ray.remote(ExecutionEngine)
        .options(num_gpus=1)
        .remote(ExecuteSchedule(0, 2), config1)
    )
    engine_actor2 = (
        ray.remote(ExecutionEngine)
        .options(num_gpus=1)
        .remote(ExecuteSchedule(1, 3), config2)
    )
    output_actor = Actor.options(num_gpus=1).remote(
        4, 3, TorchBasedCommunicator, address
    )

    ray.get(engine_actor1.start.remote())
    ray.get(engine_actor2.start.remote())

    for _ in range(2):
        tensor = torch.rand(1, 2)
        input_actor.send.remote(tensor, 1)
        output_ref = output_actor.receive.remote(2, (1, 4), tensor.dtype)
        output = ray.get(output_ref)
        assert output.shape == (1, 4)

    print("done")
    engine_actor1.stop.remote()
    engine_actor2.stop.remote()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
