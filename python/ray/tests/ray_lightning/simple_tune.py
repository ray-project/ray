# flake8: noqa

from importlib_metadata import version
from packaging.version import parse as v_parse

if v_parse(version("ray_lightning")) < v_parse("0.3.0"):  # Older Ray Lightning Version.
    import ray_lightning

    ray_lightning.RayStrategy = ray_lightning.RayPlugin

from ray.tests.ray_lightning.simple_example import LitAutoEncoder

# __train_func_begin__
import os
from torch.utils.data import DataLoader, random_split
from torchvision.datasets import MNIST
from torchvision import transforms

import pytorch_lightning as pl

from ray.tune.integration.pytorch_lightning import TuneReportCallback


def train(config):
    max_steps = 10

    dataset = MNIST(os.getcwd(), download=True, transform=transforms.ToTensor())
    train, val = random_split(dataset, [55000, 5000])

    metrics = {"loss": "train_loss"}
    autoencoder = LitAutoEncoder(lr=config["lr"])
    trainer = pl.Trainer(
        callbacks=[TuneReportCallback(metrics, on="batch_end")],
        max_steps=max_steps,
    )
    trainer.fit(autoencoder, DataLoader(train), DataLoader(val))


# __train_func_end__


def run():
    # __tune_run_begin__
    from ray import tune

    param_space = {"lr": tune.loguniform(1e-4, 1e-1)}
    num_samples = 1

    tuner = tune.Tuner(
        train,
        tune_config=tune.TuneConfig(metric="loss", mode="min", num_samples=num_samples),
        param_space=param_space,
    )

    results = tuner.fit()
    print("Best hyperparameters found were: ", results.get_best_result().config)
    # __tune_run_end__


# __train_func_distributed_begin__
import os
from torch.utils.data import DataLoader, random_split
from torchvision.datasets import MNIST
from torchvision import transforms

import pytorch_lightning as pl

from ray_lightning import RayStrategy
from ray_lightning.tune import TuneReportCallback

num_workers = 1
use_gpu = False
max_steps = 10


def train_distributed(config):
    dataset = MNIST(os.getcwd(), download=True, transform=transforms.ToTensor())
    train, val = random_split(dataset, [55000, 5000])

    metrics = {"loss": "train_loss"}
    autoencoder = LitAutoEncoder(lr=config["lr"])
    trainer = pl.Trainer(
        callbacks=[TuneReportCallback(metrics, on="batch_end")],
        strategy=RayStrategy(num_workers=num_workers, use_gpu=use_gpu),
        max_steps=max_steps,
    )
    trainer.fit(autoencoder, DataLoader(train), DataLoader(val))


# __train_func_distributed_end__


def run_distributed():
    # __tune_run_distributed_begin__
    from ray import tune
    from ray_lightning.tune import get_tune_resources

    param_space = {"lr": tune.loguniform(1e-4, 1e-1)}
    num_samples = 1

    tuner = tune.Tuner(
        tune.with_resources(
            train_distributed,
            get_tune_resources(num_workers=num_workers, use_gpu=use_gpu),
        ),
        tune_config=tune.TuneConfig(
            metric="loss",
            mode="min",
            num_samples=num_samples,
        ),
        param_space=param_space,
    )

    results = tuner.fit()

    print("Best hyperparameters found were: ", results.get_best_result().config)
    # __tune_run_distributed_end__


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Ray Lightning Example",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--distributed",
        action="store_true",
        default=False,
        help="Whether to do distributed training.",
    )

    args = parser.parse_args()

    if args.distributed:
        run_distributed()
    else:
        run()
