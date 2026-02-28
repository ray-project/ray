import argparse
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
from ray.train import RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer
from ray.train.v2.torch.torchft_config import TorchftConfig


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
    data_size = config.get("data_size", 1000)
    batch_size = config.get("batch_size", 4)
    hidden_size = config.get("hidden_size", 1)
    lr = config.get("lr", 1e-2)
    num_steps = config.get("num_steps", 100)
    report_interval = config.get("report_interval", 10)

    context = ray.train.get_context()
    world_rank = context.get_world_rank()
    world_size = context.get_world_size()
    # Each worker is its own replica group with rank 0.
    group_rank = 0
    replica_group_id = world_rank

    # Model and optimizer
    model = nn.Linear(1, hidden_size)
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)
    loss_fn = nn.MSELoss()

    # torchft process group and checkpoint transport
    pg = ProcessGroupGloo(timeout=timedelta(seconds=5))
    transport = PGTransport(
        pg,
        timeout=timedelta(seconds=10),
        device=torch.device("cpu"),
    )

    # State dict callbacks for torchft recovery
    def load_state_dict(state_dict):
        model.load_state_dict(state_dict["model"])
        optimizer.load_state_dict(state_dict["optim"])

    def state_dict():
        return {
            "model": model.state_dict(),
            "optim": optimizer.state_dict(),
        }

    manager = Manager(
        pg=pg,
        min_replica_size=1,
        load_state_dict=load_state_dict,
        state_dict=state_dict,
        world_size=1,
        rank=0,
        replica_id=f"train_ddp_{world_rank}",
        timeout=timedelta(seconds=30),
        checkpoint_transport=transport,
    )

    # Wrap model and optimizer with torchft primitives
    model = DistributedDataParallel(manager, model)
    optimizer = Optimizer(manager, optimizer)

    # Data
    train_dataset = LinearDataset(2, 5, size=data_size)
    sampler = DistributedSampler(
        train_dataset,
        replica_rank=replica_group_id,
        num_replica_groups=world_size,
        group_rank=group_rank,
        num_replicas=1,
        shuffle=False,
    )
    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=batch_size, sampler=sampler
    )

    # Training
    results = []
    train_iter = iter(train_loader)
    running_loss = 0.0
    num_batches = 0

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
        optimizer.step()
        running_loss += loss.item()
        num_batches += 1

        step = manager.current_step()
        if step % report_interval == 0 or step >= num_steps:
            avg_loss = running_loss / max(num_batches, 1)
            weight = model.module.weight.detach().flatten().tolist()
            bias = model.module.bias.detach().flatten().tolist()
            result = {"loss": avg_loss, "weight": weight, "bias": bias}
            results.append(result)
            running_loss = 0.0
            num_batches = 0

    return results


def train_torchft(num_workers=2, num_steps=100, storage_path=None):
    config = {
        "num_steps": num_steps,
    }
    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=False),
        torch_config=TorchftConfig(
            lighthouse_kwargs={"min_replicas": 1}, backend="gloo"
        ),
        run_config=RunConfig(storage_path=storage_path),
    )
    result = trainer.fit()

    print(result.metrics)
    return result.metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--num-steps", type=int, default=100, help="Number of training steps."
    )

    args, _ = parser.parse_known_args()
    train_torchft(num_workers=args.num_workers, num_steps=args.num_steps)
