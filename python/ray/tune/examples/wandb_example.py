import numpy as np
import wandb

from ray import tune
from ray.tune import Trainable
from ray.tune.integration.wandb import WandbLogger, WandbTrainableMixin
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
                "project": "Example_function"
            }
        },
        loggers=DEFAULT_LOGGERS + (WandbLogger, ))


class WandbTrainable(WandbTrainableMixin, Trainable):
    def step(self):
        for i in range(30):
            loss = self.config["mean"] + self.config["sd"] * np.random.randn()
            wandb.log({"loss": loss})
        return {"loss": loss, "done": True}


def tune_trainable():
    """Example for using a WandTrainableMixin"""
    tune.run(
        WandbTrainable,
        config={
            "mean": tune.grid_search([1, 2, 3, 4, 5]),
            "sd": tune.uniform(0.2, 0.8),
            "wandb": {
                "api_key_file": "~/.wandb_api_key",
                "project": "Example_trainable"
            }
        })


if __name__ == "__main__":
    tune_function()
    tune_trainable()
