#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import os
import random

import numpy as np

import ray
from ray.tune import Trainable, TrainingResult, register_trainable, \
    run_experiments
from ray.tune.async_hyperband import AsyncHyperBandScheduler


class MyTrainableClass(Trainable):
    """Example agent whose learning curve is a random sigmoid.

    The dummy hyperparameters "width" and "height" determine the slope and
    maximum reward value reached.
    """

    def _setup(self):
        self.timestep = 0

    def _train(self):
        self.timestep += 1
        v = np.tanh(float(self.timestep) / self.config["width"])
        v *= self.config["height"]

        # Here we use `episode_reward_mean`, but you can also report other
        # objectives such as loss or accuracy (see tune/result.py).
        return TrainingResult(episode_reward_mean=v, timesteps_this_iter=1)

    def _save(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        with open(path, "w") as f:
            f.write(json.dumps({"timestep": self.timestep}))
        return path

    def _restore(self, checkpoint_path):
        with open(checkpoint_path) as f:
            self.timestep = json.loads(f.read())["timestep"]


register_trainable("my_class", MyTrainableClass)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init()

    # asynchronous hyperband early stopping, configured with
    # `episode_reward_mean` as the
    # objective and `timesteps_total` as the time unit.
    ahb = AsyncHyperBandScheduler(
        time_attr="timesteps_total", reward_attr="episode_reward_mean",
        grace_period=5, max_t=100)

    run_experiments({
        "asynchyperband_test": {
            "run": "my_class",
            "stop": {"training_iteration": 1 if args.smoke_test else 99999},
            "repeat": 20,
            "trial_resources": {"cpu": 1, "gpu": 0},
            "config": {
                "width": lambda spec: 10 + int(90 * random.random()),
                "height": lambda spec: int(100 * random.random()),
            },
        }
    }, scheduler=ahb)
