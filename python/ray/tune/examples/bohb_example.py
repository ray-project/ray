#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import os
import random

import numpy as np
import ConfigSpace
import ConfigSpace.hyperparameters as CSH
import ConfigSpace.util

import ray
from ray.tune import Trainable, run, sample_from
from ray.tune.schedulers import TuneBOHB, HyperBandForBOHB


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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    parser.add_argument(
        "--ray-redis-address",
        help="Address of Ray cluster for seamless distributed execution.")
    args, _ = parser.parse_known_args()
    ray.init(redis_address=args.ray_redis_address)

    # asynchronous hyperband early stopping, configured with
    # `episode_reward_mean` as the
    # objective and `training_iteration` as the time unit,
    # which is automatically filled by Tune.

    # BOHB uses ConfigSpace for their hyperparameter search space
    CS = ConfigSpace
    config_space = CS.ConfigurationSpace()
    config_space.add_hyperparameter(
        CS.UniformFloatHyperparameter('height', lower=10, upper=100))
    config_space.add_hyperparameter(
        CS.UniformFloatHyperparameter('width', lower=0, upper=100))

    bohb = HyperBandForBOHB(
        time_attr="training_iteration",
        metric="episode_reward_mean",
        grace_period=5,
        max_t=100)

    run(MyTrainableClass,
        name="bohb_test",
        scheduler=bohb,
        search_alg=TuneBOHB(
            config_space,
            max_concurrent=4,
            # num_concurrent=100,
            reward_attr="episode_reward_mean",
        ),
        **{
            "stop": {
                "training_iteration": 1 if args.smoke_test else 99999
            },
            "num_samples": 20,
            "resources_per_trial": {
                "cpu": 1,
                "gpu": 0
            },
        })
