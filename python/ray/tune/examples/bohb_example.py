#!/usr/bin/env python

import json
import os

import numpy as np

import ray
from ray.tune import Trainable, run
from ray.tune.schedulers.hb_bohb import HyperBandForBOHB
from ray.tune.suggest.bohb import TuneBOHB


class MyTrainableClass(Trainable):
    """Example agent whose learning curve is a random sigmoid.

    The dummy hyperparameters "width" and "height" determine the slope and
    maximum reward value reached.
    """

    def _setup(self, config):
        self.timestep = 0

    def _train(self):
        self.timestep += 1
        v = np.tanh(float(self.timestep) / self.config.get("width", 1))
        v *= self.config.get("height", 1)

        # Here we use `episode_reward_mean`, but you can also report other
        # objectives such as loss or accuracy.
        return {"episode_reward_mean": v}

    def _save(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        with open(path, "w") as f:
            f.write(json.dumps({"timestep": self.timestep}))
        return path

    def _restore(self, checkpoint_path):
        with open(checkpoint_path) as f:
            self.timestep = json.loads(f.read())["timestep"]


if __name__ == "__main__":
    import ConfigSpace as CS
    ray.init(num_cpus=8)

    # BOHB uses ConfigSpace for their hyperparameter search space
    config_space = CS.ConfigurationSpace()
    config_space.add_hyperparameter(
        CS.UniformFloatHyperparameter("height", lower=10, upper=100))
    config_space.add_hyperparameter(
        CS.UniformFloatHyperparameter("width", lower=0, upper=100))

    experiment_metrics = dict(metric="episode_reward_mean", mode="max")
    bohb_hyperband = HyperBandForBOHB(
        time_attr="training_iteration",
        max_t=100,
        reduction_factor=4,
        **experiment_metrics)
    bohb_search = TuneBOHB(
        config_space, max_concurrent=4, **experiment_metrics)

    run(MyTrainableClass,
        name="bohb_test",
        scheduler=bohb_hyperband,
        search_alg=bohb_search,
        num_samples=10,
        stop={"training_iteration": 100})
