# This example mirrors python/ray/train/v2/examples/pytorch/torchft_linear_example.py,
# which CI tests with torchft installed (test_torchft_linear in
# python/ray/train/v2/tests/test_torch_trainer.py). This file is excluded from the
# doc code test target because the doc CI image doesn't have torchft.

# __torchft_quickstart_start__
from datetime import timedelta

import numpy as np
import torch
import torch.nn as nn
from torchft import (
    DistributedDataParallel,
    DistributedSampler,
    Manager,
    Optimizer,
    ProcessGroupGloo,
)
from torchft.checkpointing.pg_transport import PGTransport

import ray.train
from ray.train import FailureConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchftConfig, TorchTrainer


class LinearDataset(torch.utils.data.Dataset):
    """y = a * x + b"""

    def __init__(self, a, b, size=1000):
        x = np.arange(0, 10, 10 / size, dtype=np.float32)
        self.x = torch.from_numpy(x)
        self.y = torch.from_numpy(a * x + b)

    def __getitem__(self, index):
        return self.x[index, None], self.y[index, None]

    def __len__(self):
        return len(self.x)


def train_func(config):
    num_steps = config.get("num_steps", 100)
    min_replicas = config.get("min_replicas", 1)

    context = ray.train.get_context()
    world_rank = context.get_world_rank()
    world_size = context.get_world_size()

    model = nn.Linear(1, 1)
    optimizer = torch.optim.SGD(model.parameters(), lr=1e-2)
    loss_fn = nn.MSELoss()

    # torchft process group and checkpoint transport. Use ProcessGroupNCCL
    # and device="cuda" on GPU. Timeouts must be generous enough to re-form
    # the process group after a replica fails.
    pg = ProcessGroupGloo(timeout=timedelta(seconds=30))
    transport = PGTransport(
        pg,
        timeout=timedelta(seconds=30),
        device=torch.device("cpu"),
    )

    # State dict callbacks that torchft uses to recover a replacement
    # replica group from a live peer. Include both the model and the
    # optimizer state.
    def load_state_dict(state_dict):
        model.load_state_dict(state_dict["model"])
        optimizer.load_state_dict(state_dict["optim"])

    def state_dict():
        return {
            "model": model.state_dict(),
            "optim": optimizer.state_dict(),
        }

    # Each Ray Train worker is its own replica group, so the replica-group
    # world size is 1, the in-group rank is 0, and the replica ID comes
    # from the Ray Train world rank.
    manager = Manager(
        pg=pg,
        min_replica_size=min_replicas,
        load_state_dict=load_state_dict,
        state_dict=state_dict,
        world_size=1,
        rank=0,
        replica_id=f"train_ddp_{world_rank}",
        timeout=timedelta(seconds=60),
        checkpoint_transport=transport,
    )

    # Wrap the model and optimizer with the torchft primitives instead of
    # calling ray.train.torch.prepare_model.
    model = DistributedDataParallel(manager, model)
    optimizer = Optimizer(manager, optimizer)

    # Shard the data across replica groups with the torchft
    # DistributedSampler instead of ray.train.torch.prepare_data_loader.
    train_dataset = LinearDataset(2, 5)
    sampler = DistributedSampler(
        train_dataset,
        replica_rank=world_rank,
        num_replica_groups=world_size,
        group_rank=0,
        num_replicas=1,
        shuffle=False,
    )
    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=4, sampler=sampler
    )

    # Train for a fixed number of steps. manager.current_step() advances
    # across failures, so replacement workers don't start over.
    train_iter = iter(train_loader)
    while manager.current_step() < num_steps:
        try:
            X, y = next(train_iter)
        except StopIteration:
            train_iter = iter(train_loader)
            X, y = next(train_iter)

        optimizer.zero_grad()
        pred = model(X)
        loss = loss_fn(pred, y)
        loss.backward()
        # The wrapped optimizer commits the step only if the whole quorum
        # finished it. Otherwise, torchft skips the step.
        optimizer.step()

    # Report final metrics once, when all workers are present.
    ray.train.report({"loss": loss.item()})

    # Shut down the manager so a worker that rejoins late doesn't wait
    # forever for peers that already finished.
    manager.shutdown()


trainer = TorchTrainer(
    train_func,
    train_loop_config={"num_steps": 100, "min_replicas": 1},
    scaling_config=ScalingConfig(num_workers=2),
    # min_replicas: how many replica groups must stay alive for training
    # to continue while Ray Train replaces the failed ones.
    torch_config=TorchftConfig(lighthouse_kwargs={"min_replicas": 1}, backend="gloo"),
    # max_failures must be at least 1. Otherwise, the first failure ends
    # the run instead of replacing the failed replica group.
    run_config=RunConfig(failure_config=FailureConfig(max_failures=3)),
)
result = trainer.fit()
# __torchft_quickstart_end__
