#!/usr/bin/env python

import argparse
import json
import os

import numpy as np

import ray
from ray import air, tune
from ray.air import session
from ray.air.checkpoint import Checkpoint
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
        checkpoint = None
        if timestep % 3 == 0:
            checkpoint = Checkpoint.from_dict({"timestep": timestep})

        # Here we use `episode_reward_mean`, but you can also report other
        # objectives such as loss or accuracy.
        session.report({"episode_reward_mean": v}, checkpoint=checkpoint)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

    ray.init(num_cpus=4 if args.smoke_test else None)

    # Hyperband early stopping, configured with `episode_reward_mean` as the
    # objective and `training_iteration` as the time unit,
    # which is automatically filled by Tune.
    hyperband = HyperBandScheduler(max_t=200)

    tuner = tune.Tuner(
        train,
        run_config=air.RunConfig(
            name="hyperband_test",
            stop={"training_iteration": 10 if args.smoke_test else 99999},
            failure_config=air.FailureConfig(
                fail_fast=True,
            ),
        ),
        tune_config=tune.TuneConfig(
            num_samples=20,
            metric="episode_reward_mean",
            mode="max",
            scheduler=hyperband,
        ),
        param_space={"height": tune.uniform(0, 100)},
    )
    results = tuner.fit()
    print("Best hyperparameters found were: ", results.get_best_result().config)
