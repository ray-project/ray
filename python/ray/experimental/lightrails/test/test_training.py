from typing import Any

import pytest
import ray
import torch
from ray.experimental.lightrails.communicator import (
    NaiveCommunicator,
    TorchBasedCommunicator,
)
from ray.experimental.lightrails.engine import Config, ExecutionEngine
from ray.experimental.lightrails.schedule.training import (
    FirstStageSchedule,
    LastStageSchedule,
    MiddleStageSchedule,
)
from ray.experimental.lightrails.test.utils import (
    Actor,
    Model,
    ray_start_4_cpus_2_gpus,
    ray_start_auto,
)
from ray.tests.conftest import *  # noqa
from torch.utils.data import DataLoader, TensorDataset


def test_pipeline_training(ray_start_4_cpus_2_gpus):
    # training a linear model Y = 35 * X + 4
    # with unnecessary 3 stages
    num_batches = 100

    x_train = torch.randn((num_batches, 1))  # Input features
    y_train = x_train * 35
    print(f"trainging data: {x_train}, {y_train}")

    dataset = TensorDataset(x_train, y_train)
    dataloader = DataLoader(dataset, batch_size=1)

    config1 = Config(
        world_size=3,
        rank=0,
        input_tensor_shape=(1, 1),
        input_tensor_dtype=torch.float32,
        device_name_builder=lambda: "cpu",
        communicator_builder=lambda world_size, rank, master_addr: NaiveCommunicator(
            world_size, rank, master_addr=master_addr
        ),
        model_builder=lambda: Model(1, 1),
        data_loader_builder=lambda: dataloader,
        optimizer_builder=lambda model: torch.optim.SGD(model.parameters(), lr=0.01),
    )

    config2 = Config(
        world_size=3,
        rank=1,
        input_tensor_shape=(1, 1),
        input_tensor_dtype=torch.float32,
        device_name_builder=lambda: "cpu",
        communicator_builder=lambda world_size, rank, master_addr: NaiveCommunicator(
            world_size, rank, master_addr=master_addr
        ),
        model_builder=lambda: Model(1, 1),
        data_loader_builder=lambda: None,
        optimizer_builder=lambda model: torch.optim.SGD(model.parameters(), lr=0.01),
    )

    config3 = Config(
        world_size=3,
        rank=2,
        input_tensor_shape=(1, 1),
        input_tensor_dtype=torch.float32,
        device_name_builder=lambda: "cpu",
        communicator_builder=lambda world_size, rank, master_addr: NaiveCommunicator(
            world_size, rank, master_addr=master_addr
        ),
        model_builder=lambda: Model(1, 1, loss_fn=torch.nn.MSELoss()),
        data_loader_builder=lambda: dataloader,
        optimizer_builder=lambda model: torch.optim.SGD(model.parameters(), lr=0.01),
    )

    actor1 = ray.remote(ExecutionEngine).remote(
        schedule=FirstStageSchedule(downstream_rank=1, num_batches=num_batches),
        config=config1,
        is_training=True,
    )
    actor2 = ray.remote(ExecutionEngine).remote(
        schedule=MiddleStageSchedule(
            upstream_rank=0, downstream_rank=2, num_batches=num_batches
        ),
        config=config2,
        is_training=True,
    )
    actor3 = ray.remote(ExecutionEngine).remote(
        schedule=LastStageSchedule(upstream_rank=1, num_batches=num_batches),
        config=config3,
        is_training=True,
        is_last_trainig_stage=True,
    )
    actors = [actor1, actor2, actor3]
    address = ray.get(actors[0].get_address.remote())

    print("Starting engines")
    # Start the engines and also starts the training.
    ray.get([actor.start.remote(address) for actor in actors])

    print("run trainings")

    # Wait for the training to finish.
    ray.get([actor.wait_until_stopped.remote() for actor in actors])
    print("Finished training")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
