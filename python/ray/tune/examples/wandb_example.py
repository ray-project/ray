import argparse
from unittest.mock import MagicMock

import numpy as np
import wandb

from ray import tune
from ray.tune import Trainable
from ray.tune.integration.wandb import WandbLogger, WandbTrainableMixin, \
    wandb_mixin
from ray.tune.logger import DEFAULT_LOGGERS


def train_function(config, checkpoint=None):
    for i in range(30):
        loss = config["mean"] + config["sd"] * np.random.randn()
        tune.report(loss=loss)


def tune_function():
    """Example for using a WandbLogger with the function API"""
    tune.run(
        train_function,
        config={
            "mean": tune.grid_search([1, 2, 3, 4, 5]),
            "sd": tune.uniform(0.2, 0.8),
            "wandb": {
                "api_key_file": "~/.wandb_api_key",
                "project": "Wandb_example"
            }
        },
        loggers=DEFAULT_LOGGERS + (WandbLogger, ))


@wandb_mixin
def decorated_train_function(config, checkpoint=None):
    for i in range(30):
        loss = config["mean"] + config["sd"] * np.random.randn()
        tune.report(loss=loss)
        wandb.log(dict(loss=loss))


def tune_decorated():
    """Example for using the @wandb_mixin decorator with the function API"""
    tune.run(
        decorated_train_function,
        config={
            "mean": tune.grid_search([1, 2, 3, 4, 5]),
            "sd": tune.uniform(0.2, 0.8),
            "wandb": {
                "api_key_file": "~/.wandb_api_key",
                "project": "Wandb_example"
            }
        })


class WandbTrainable(WandbTrainableMixin, Trainable):
    def step(self):
        for i in range(30):
            loss = self.config["mean"] + self.config["sd"] * np.random.randn()
            wandb.log({"loss": loss})
        return {"loss": loss, "done": True}


def tune_trainable():
    """Example for using a WandTrainableMixin with the class API"""
    tune.run(
        WandbTrainable,
        config={
            "mean": tune.grid_search([1, 2, 3, 4, 5]),
            "sd": tune.uniform(0.2, 0.8),
            "wandb": {
                "api_key_file": "~/.wandb_api_key",
                "project": "Wandb_example"
            }
        })


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mock-api", action="store_true", help="Mock Wandb API access")
    args, _ = parser.parse_known_args()

    if args.mock_api:
        WandbLogger._logger_process_cls = MagicMock
        decorated_train_function.__mixins__ = tuple()
        WandbTrainable._wandb = MagicMock()
        wandb = MagicMock()  # noqa: F811

    tune_function()
    tune_decorated()
    tune_trainable()
