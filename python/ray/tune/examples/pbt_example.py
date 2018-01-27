#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import os
import random
import time

import numpy as np

import ray
from ray.tune import Trainable, TrainingResult, register_trainable, \
    run_experiments, grid_search
from ray.tune.pbt import PopulationBasedTraining


class MyTrainableClass(Trainable):
    """Fake agent whose learning rate is determined by dummy params."""

    def _setup(self):
        self.timestep = 0
        self.current_value = 0.0

    def _train(self):
        time.sleep(0.1)

        # Reward increase is parabolic as a function of factor_2, with a
        # maximia around factor_1=10.0.
        self.current_value += max(
            0.0, random.gauss(5.0 - (self.config["factor_1"] - 10.0)**2, 2.0))

        # Flat increase by factor_2
        self.current_value += random.gauss(self.config["factor_2"], 1.0)

        # Here we use `episode_reward_mean`, but you can also report other
        # objectives such as loss or accuracy (see tune/result.py).
        return TrainingResult(
            episode_reward_mean=self.current_value, timesteps_this_iter=1)

    def _save(self):
        path = os.path.join(self.logdir, "checkpoint")
        with open(path, "w") as f:
            f.write(json.dumps(
                {"timestep": self.timestep, "value": self.current_value}))
        return path

    def _restore(self, checkpoint_path):
        with open(checkpoint_path) as f:
            data = json.loads(f.read())
            self.timestep = data["timestep"]
            self.current_value = data["value"]


register_trainable("my_class", MyTrainableClass)

if __name__ == "__main__":
    ray.init()

    pbt = PopulationBasedTraining(
        time_attr="training_iteration", reward_attr="episode_reward_mean",
        perturbation_interval=10,
        hyperparam_mutations={
            # Allow for scaling-based perturbations, with a uniform backing
            # distribution for resampling.
            "factor_1": lambda config: np.random.uniform(0.0, 20.0),
            # Only allows resampling from this list as a perturbation.
            "factor_2": [1, 2],
        })

    # Try to find the best factor 1 and factor 2
    run_experiments({
        "pbt_test": {
            "run": "my_class",
            "repeat": 10,
            "resources": {"cpu": 1, "gpu": 0},
            "config": {
                "factor_1": 4.0,
                "factor_2": 1.0,
            },
        }
    }, scheduler=pbt, verbose=False)
