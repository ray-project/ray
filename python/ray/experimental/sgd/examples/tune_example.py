# yapf: disable
"""
This file holds code for a Distributed Pytorch + Tune page in the docs.

It ignores yapf because yapf doesn't allow comments right after code blocks,
but we put comments right after code blocks to prevent large white spaces
in the documentation.
"""

# __torch_tune_example__
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import torch
import torch.nn as nn
from torch import distributed
from torch.utils.data.distributed import DistributedSampler

import ray
from ray import tune
from ray.experimental.sgd.pytorch.pytorch_trainer import PyTorchTrainable


class LinearDataset(torch.utils.data.Dataset):
    """y = a * x + b"""

    def __init__(self, a, b, size=1000):
        x = np.random.random(size).astype(np.float32) * 10
        x = np.arange(0, 10, 10 / size, dtype=np.float32)
        self.x = torch.from_numpy(x)
        self.y = torch.from_numpy(a * x + b)

    def __getitem__(self, index):
        return self.x[index, None], self.y[index, None]

    def __len__(self):
        return len(self.x)


def model_creator(config):
    return nn.Linear(1, 1)


def optimizer_creator(model, config):
    """Returns optimizer."""
    return torch.optim.SGD(model.parameters(), lr=config.get("lr", 1e-4))


def data_creator(batch_size, config):
    """Returns training dataloader, validation dataloader."""
    train_dataset = LinearDataset(2, 5)
    validation_dataset = LinearDataset(2, 5, size=400)

    train_sampler = None
    if distributed.is_initialized():
        train_sampler = DistributedSampler(train_dataset)
    train_loader = torch.utils.data.DataLoader(
        train_dataset,
        batch_size=batch_size,
        shuffle=(train_sampler is None),
        sampler=train_sampler)

    validation_sampler = None
    if distributed.is_initialized():
        validation_sampler = DistributedSampler(validation_dataset)
    validation_loader = torch.utils.data.DataLoader(
        validation_dataset,
        batch_size=batch_size,
        shuffle=(validation_sampler is None),
        sampler=validation_sampler)
    return train_loader, validation_loader


def tune_example(num_replicas=1, use_gpu=False):
    config = {
        "model_creator": tune.function(model_creator),
        "data_creator": tune.function(data_creator),
        "optimizer_creator": tune.function(optimizer_creator),
        "loss_creator": tune.function(lambda config: nn.MSELoss()),
        "num_replicas": num_replicas,
        "use_gpu": use_gpu,
        "batch_size": 512,
        "backend": "gloo"
    }

    analysis = tune.run(
        PyTorchTrainable,
        num_samples=12,
        config=config,
        stop={"training_iteration": 2},
        verbose=1)

    return analysis.get_best_config(metric="validation_loss", mode="min")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--redis-address",
        type=str,
        help="the address to use for Redis")
    parser.add_argument(
        "--num-replicas",
        "-n",
        type=int,
        default=1,
        help="Sets number of replicas for training.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")
    parser.add_argument(
        "--tune", action="store_true", default=False, help="Tune training")

    args, _ = parser.parse_known_args()

    ray.init(redis_address=args.redis_address)
    tune_example(num_replicas=args.num_replicas, use_gpu=args.use_gpu)
