#!/usr/bin/env python

"""This example demonstrates the usage of BOHB with Ray Tune.

Requires the HpBandSter and ConfigSpace libraries to be installed
(`pip install hpbandster ConfigSpace`).
"""

import json
import os
import time

import numpy as np

import ray
from ray import tune
from ray.tune import Trainable
from ray.tune.schedulers.hb_bohb import HyperBandForBOHB
from ray.tune.search.bohb import TuneBOHB


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

    def load_checkpoint(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        with open(path, "r") as f:
            self.timestep = json.loads(f.read())["timestep"]


if __name__ == "__main__":
    import sys

    if sys.version_info >= (3, 12):
        # TuneBOHB is not compatible with Python 3.12
        sys.exit(0)

    ray.init(num_cpus=8)

    config = {
        "iterations": 100,
        "width": tune.uniform(0, 20),
        "height": tune.uniform(-100, 100),
        "activation": tune.choice(["relu", "tanh"]),
    }

    # Optional: Pass the parameter space yourself
    # import ConfigSpace as CS
    # config_space = CS.ConfigurationSpace()
    # config_space.add_hyperparameter(
    #     CS.UniformFloatHyperparameter("width", lower=0, upper=20))
    # config_space.add_hyperparameter(
    #     CS.UniformFloatHyperparameter("height", lower=-100, upper=100))
    # config_space.add_hyperparameter(
    #     CS.CategoricalHyperparameter(
    #         "activation", choices=["relu", "tanh"]))

    max_iterations = 10
    bohb_hyperband = HyperBandForBOHB(
        time_attr="training_iteration",
        max_t=max_iterations,
        reduction_factor=2,
        stop_last_trials=False,
    )

    bohb_search = TuneBOHB(
        # space=config_space,  # If you want to set the space manually
    )
    bohb_search = tune.search.ConcurrencyLimiter(bohb_search, max_concurrent=4)

    tuner = tune.Tuner(
        MyTrainableClass,
        run_config=tune.RunConfig(
            name="bohb_test", stop={"training_iteration": max_iterations}
        ),
        tune_config=tune.TuneConfig(
            metric="episode_reward_mean",
            mode="max",
            scheduler=bohb_hyperband,
            search_alg=bohb_search,
            num_samples=32,
        ),
        param_space=config,
    )
    results = tuner.fit()

    print("Best hyperparameters found were: ", results.get_best_result().config)
