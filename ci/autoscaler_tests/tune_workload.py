#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import os
import random
import time

import numpy as np

import ray
from ray import tune
from ray.tune import Trainable, run_experiments, Experiment


class MyTrainableClass(Trainable):
    """Example agent whose learning curve is a random sigmoid.

    The dummy hyperparameters "width" and "height" determine the slope and
    maximum reward value reached.
    """

    def _setup(self, config):
        self.timestep = 0

    def _train(self):
        self.timestep += 1
        v = np.tanh(float(self.timestep) / self.config["width"])
        v *= self.config["height"]
        time.sleep(0.1)

        # Here we use `episode_reward_mean`, but you can also report other
        # objectives such as loss or accuracy.
        return {"episode_reward_mean": v}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cluster", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    if args.cluster:
        ray.init(redis_address="localhost:6379")
    else:
        ray.init()
    exp = Experiment(
        name="tune-workload",
        run=MyTrainableClass,
        num_samples=200,
        stop={"training_iteration": 100},
        config={
            "width": tune.sample_from(
                lambda spec: 10 + int(90 * random.random())),
            "height": tune.sample_from(lambda spec: int(100 * random.random()))
        })

    trials = run_experiments(exp)
