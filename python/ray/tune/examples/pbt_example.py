#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import os
import random
import time

import ray
from ray.tune import Trainable, run
from ray.tune.schedulers import PopulationBasedTraining


class MyTrainableClass(Trainable):
    """Fake agent whose learning rate is determined by dummy factors."""

    def _setup(self, config):
        self.timestep = 0
        self.current_value = 0.0

    def _train(self):
        time.sleep(0.1)

        # Reward increase is parabolic as a function of factor_2, with a
        # maxima around factor_1=10.0.
        self.current_value += max(
            0.0, random.gauss(5.0 - (self.config["factor_1"] - 10.0)**2, 2.0))

        # Flat increase by factor_2
        self.current_value += random.gauss(self.config["factor_2"], 1.0)

        # Here we use `episode_reward_mean`, but you can also report other
        # objectives such as loss or accuracy.
        return {"episode_reward_mean": self.current_value}

    def _save(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        with open(path, "w") as f:
            f.write(
                json.dumps({
                    "timestep": self.timestep,
                    "value": self.current_value
                }))
        return path

    def _restore(self, checkpoint_path):
        with open(checkpoint_path) as f:
            data = json.loads(f.read())
            self.timestep = data["timestep"]
            self.current_value = data["value"]

    def reset_config(self, new_config):
        self.config = new_config
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    if args.smoke_test:
        ray.init(num_cpus=4)  # force pausing to happen for test
    else:
        ray.init()

    pbt = PopulationBasedTraining(
        time_attr="training_iteration",
        reward_attr="episode_reward_mean",
        perturbation_interval=10,
        hyperparam_mutations={
            # Allow for scaling-based perturbations, with a uniform backing
            # distribution for resampling.
            "factor_1": lambda: random.uniform(0.0, 20.0),
            # Allow perturbations within this set of categorical values.
            "factor_2": [1, 2],
        })

    # Try to find the best factor 1 and factor 2
    run(MyTrainableClass,
        name="pbt_test",
        scheduler=pbt,
        reuse_actors=True,
        verbose=False,
        **{
            "stop": {
                "training_iteration": 20 if args.smoke_test else 99999
            },
            "num_samples": 10,
            "config": {
                "factor_1": 4.0,
                "factor_2": 1.0,
            },
        })
