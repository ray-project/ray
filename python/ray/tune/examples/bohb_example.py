#!/usr/bin/env python

import argparse
import json
import time
import os

import numpy as np

import ray
from ray import tune
from ray.tune import Trainable
from ray.tune.schedulers.hb_bohb import HyperBandForBOHB
from ray.tune.suggest.bohb import TuneBOHB


class MyTrainableClass(Trainable):
    """Example agent whose learning curve is a random sigmoid.

    The dummy hyperparameters "width" and "height" determine the slope and
    maximum reward value reached.
    """

    def setup(self, config):
        self.timestep = 0

    def step(self):
        self.timestep += 1
        v = np.tanh(float(self.timestep) / self.config.get("width", 1))
        v *= self.config.get("height", 1)
        time.sleep(0.1)
        # Here we use `episode_reward_mean`, but you can also report other
        # objectives such as loss or accuracy.
        return {"episode_reward_mean": v}

    def save_checkpoint(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        with open(path, "w") as f:
            f.write(json.dumps({"timestep": self.timestep}))
        return path

    def load_checkpoint(self, checkpoint_path):
        with open(checkpoint_path) as f:
            self.timestep = json.loads(f.read())["timestep"]


if __name__ == "__main__":
    import ConfigSpace as CS  # noqa: F401

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using Ray Client.",
    )
    args, _ = parser.parse_known_args()

    if args.server_address:
        ray.init(f"ray://{args.server_address}")
    else:
        ray.init(num_cpus=8)

    config = {
        "iterations": 100,
        "width": tune.uniform(0, 20),
        "height": tune.uniform(-100, 100),
        "activation": tune.choice(["relu", "tanh"]),
    }

    # Optional: Pass the parameter space yourself
    # config_space = CS.ConfigurationSpace()
    # config_space.add_hyperparameter(
    #     CS.UniformFloatHyperparameter("width", lower=0, upper=20))
    # config_space.add_hyperparameter(
    #     CS.UniformFloatHyperparameter("height", lower=-100, upper=100))
    # config_space.add_hyperparameter(
    #     CS.CategoricalHyperparameter(
    #         "activation", choices=["relu", "tanh"]))

    bohb_hyperband = HyperBandForBOHB(
        time_attr="training_iteration",
        max_t=100,
        reduction_factor=4,
        stop_last_trials=False,
    )

    bohb_search = TuneBOHB(
        # space=config_space,  # If you want to set the space manually
    )
    bohb_search = tune.suggest.ConcurrencyLimiter(bohb_search, max_concurrent=4)

    analysis = tune.run(
        MyTrainableClass,
        name="bohb_test",
        config=config,
        scheduler=bohb_hyperband,
        search_alg=bohb_search,
        num_samples=10,
        stop={"training_iteration": 100},
        metric="episode_reward_mean",
        mode="max",
    )

    print("Best hyperparameters found were: ", analysis.best_config)
