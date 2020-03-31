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
from ray.util.sgd.torch import TorchTrainer
from ray.util.sgd.utils import BATCH_SIZE


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
    train_dataset = LinearDataset(2, 5)
    val_dataset = LinearDataset(2, 5, size=400)
    train_loader = torch.utils.data.DataLoader(
        train_dataset,
        batch_size=config[BATCH_SIZE],
    )
    validation_loader = torch.utils.data.DataLoader(
        val_dataset,
        batch_size=config[BATCH_SIZE])
    return train_loader, validation_loader


def tune_example(num_workers=1, use_gpu=False):
    TorchTrainable = TorchTrainer.as_trainable(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=nn.MSELoss,
        num_workers=num_workers,
        use_gpu=use_gpu,
        config={BATCH_SIZE: 128}
    )

    analysis = tune.run(
        TorchTrainable,
        num_samples=3,
        config={"lr": tune.grid_search([1e-4, 1e-3])},
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
        "--num-workers",
        "-n",
        type=int,
        default=1,
        help="Sets number of workers for training.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")

    args, _ = parser.parse_known_args()

    ray.init(address=args.address)
    tune_example(num_workers=args.num_workers, use_gpu=args.use_gpu)
