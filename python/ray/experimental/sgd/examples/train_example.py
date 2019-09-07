"""
This file holds code for a Training guide for PytorchSGD in the documentation.

It ignores yapf because yapf doesn't allow comments right after code blocks,
but we put comments right after code blocks to prevent large white spaces
in the documentation.
"""

# yapf: disable
# __torch_train_example__
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import numpy as np
import torch
import torch.nn as nn

from ray.experimental.sgd.pytorch.pytorch_trainer import PyTorchTrainer


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
    """Returns criterion, optimizer"""
    criterion = nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=1e-4)
    return criterion, optimizer


def data_creator(config):
    """Returns training set, validation set"""
    return LinearDataset(2, 5), LinearDataset(2, 5, size=400)


def train_example(num_replicas=1, use_gpu=False):
    trainer1 = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        num_replicas=num_replicas,
        use_gpu=use_gpu,
        batch_size=512,
        backend="gloo")
    trainer1.train()
    trainer1.shutdown()
    print("success!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--redis-address",
        required=False,
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

    import ray

    ray.init(redis_address=args.redis_address)
    train_example(num_replicas=args.num_replicas, use_gpu=args.use_gpu)
