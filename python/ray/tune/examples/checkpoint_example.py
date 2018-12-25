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
from ray.tune import Trainable, run_experiments, Experiment, sample_from
from ray.tune.schedulers import HyperBandScheduler


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
        time.sleep(1)
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
    args, _ = parser.parse_known_args()
    ray.init(num_cpus=4)

    exp = Experiment(
        name="hyperband_test",
        run=MyTrainableClass,
        num_samples=20,
        stop={"training_iteration": 50},
        checkpoint_freq=4,
        local_dir="~/ray_results/checkpoint_test/",
        config={
            "width": sample_from(lambda spec: 10 + int(90 * random.random())),
            "height": sample_from(lambda spec: int(100 * random.random()))
        })

    run_experiments(exp, resume=True)
