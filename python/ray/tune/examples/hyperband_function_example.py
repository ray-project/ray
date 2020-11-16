#!/usr/bin/env python

import argparse
import json
import os

import numpy as np

import ray
from ray import tune
from ray.tune.schedulers import HyperBandScheduler


def train(config, checkpoint_dir=None):
    step = 0
    if checkpoint_dir:
        with open(os.path.join(checkpoint_dir, "checkpoint")) as f:
            step = json.loads(f.read())["timestep"]

    for timestep in range(step, 100):
        v = np.tanh(float(timestep) / config.get("width", 1))
        v *= config.get("height", 1)

        # Checkpoint the state of the training every 3 steps
        # Note that this is only required for certain schedulers
        if timestep % 3 == 0:
            with tune.checkpoint_dir(step=timestep) as checkpoint_dir:
                path = os.path.join(checkpoint_dir, "checkpoint")
                with open(path, "w") as f:
                    f.write(json.dumps({"timestep": timestep}))

        # Here we use `episode_reward_mean`, but you can also report other
        # objectives such as loss or accuracy.
        tune.report(episode_reward_mean=v)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init(num_cpus=4 if args.smoke_test else None)

    # Hyperband early stopping, configured with `episode_reward_mean` as the
    # objective and `training_iteration` as the time unit,
    # which is automatically filled by Tune.
    hyperband = HyperBandScheduler(max_t=200)

    analysis = tune.run(
        train,
        name="hyperband_test",
        num_samples=20,
        metric="episode_reward_mean",
        mode="max",
        stop={"training_iteration": 10 if args.smoke_test else 99999},
        config={"height": tune.uniform(0, 100)},
        scheduler=hyperband,
        fail_fast=True)
    print("Best hyperparameters found were: ", analysis.best_config)
