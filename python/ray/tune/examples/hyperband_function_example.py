#!/usr/bin/env python

import argparse
import json
import os

import numpy as np

import ray
from ray import tune
from ray.tune.schedulers import HyperBandScheduler


def train(config, checkpoint=None):
    step = 0
    if checkpoint:
        with open(checkpoint) as f:
            step = json.loads(f.read())["timestep"]

    for timestep in range(step, 100):
        v = np.tanh(float(timestep) / config.get("width", 1))
        v *= config.get("height", 1)

        if timestep % 3 == 0:
            checkpoint_dir = tune.make_checkpoint_dir(step=timestep)
            path = os.path.join(checkpoint_dir, "checkpoint")
            with open(path, "w") as f:
                f.write(json.dumps({"timestep": timestep}))
            tune.save_checkpoint(path)

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
    hyperband = HyperBandScheduler(
        time_attr="training_iteration",
        metric="episode_reward_mean",
        mode="max",
        max_t=200)

    tune.run(
        train,
        name="hyperband_test",
        num_samples=20,
        stop={"training_iteration": 10 if args.smoke_test else 99999},
        config={"height": tune.uniform(0, 100)},
        scheduler=hyperband,
        fail_fast=True)
