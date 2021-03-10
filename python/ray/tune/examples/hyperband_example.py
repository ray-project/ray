#!/usr/bin/env python

import argparse
import json
import os
import time

import numpy as np

import ray
from ray import tune
from ray.tune.schedulers import HyperBandScheduler


class MyTrainableClass(tune.Trainable):
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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init(num_cpus=4 if args.smoke_test else None)

    # Hyperband early stopping, configured with `episode_reward_mean` as the
    # objective and `training_iteration` as the time unit,
    # which is automatically filled by Tune.
    hyperband = HyperBandScheduler(time_attr="training_iteration", max_t=200)

    analysis = tune.run(
        MyTrainableClass,
        name="hyperband_test",
        num_samples=20 if args.smoke_test else 200,
        metric="episode_reward_mean",
        mode="max",
        stop={"training_iteration": 1 if args.smoke_test else 200},
        config={
            "width": tune.randint(10, 90),
            "height": tune.randint(0, 100)
        },
        verbose=1,
        scheduler=hyperband,
        fail_fast=True)

    print("Best hyperparameters found were: ", analysis.best_config)
