# yapf: disable
"""
This file holds code for a Distributed Pytorch + Tune page in the docs.

It ignores yapf because yapf doesn't allow comments right after code blocks,
but we put comments right after code blocks to prevent large white spaces
in the documentation.
"""

# __torch_tune_example__
import numpy as np
import torch
import torch.nn as nn

import ray
from ray import tune
from ray.util.sgd.pytorch.pytorch_trainer import PyTorchTrainable


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


def data_creator(config):
    """Returns training dataloader, validation dataloader."""
    return LinearDataset(2, 5), LinearDataset(2, 5, size=400)


def tune_example(num_replicas=1, use_gpu=False):
    config = {
        "model_creator": tune.function(model_creator),
        "data_creator": tune.function(data_creator),
        "optimizer_creator": tune.function(optimizer_creator),
        "loss_creator": tune.function(nn.MSELoss),
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
        "--address",
        type=str,
        help="the address to use for Ray")
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

    ray.init(address=args.address)
    tune_example(num_replicas=args.num_replicas, use_gpu=args.use_gpu)
