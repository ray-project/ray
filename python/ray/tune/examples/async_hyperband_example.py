#!/usr/bin/env python

import argparse
import time

import ray
from ray import tune
from ray.tune.schedulers import AsyncHyperBandScheduler


def evaluation_fn(step, width, height):
    time.sleep(0.1)
    return (0.1 + width * step / 100)**(-1) + height * 0.1


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config["steps"]):
        # Iterative training function - can be an arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back back to Tune.
        tune.report(iterations=step, mean_loss=intermediate_score)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    parser.add_argument(
        "--ray-address",
        help="Address of Ray cluster for seamless distributed execution.",
        required=False)
    args, _ = parser.parse_known_args()
    ray.init(address=args.ray_address)

    # AsyncHyperBand enables aggressive early stopping of bad trials.
    scheduler = AsyncHyperBandScheduler(grace_period=5, max_t=100)

    # 'training_iteration' is incremented every time `trainable.step` is called
    stopping_criteria = {"training_iteration": 1 if args.smoke_test else 9999}

    analysis = tune.run(
        easy_objective,
        name="asynchyperband_test",
        metric="mean_loss",
        mode="min",
        scheduler=scheduler,
        stop=stopping_criteria,
        num_samples=20,
        verbose=1,
        resources_per_trial={
            "cpu": 1,
            "gpu": 0
        },
        config={  # Hyperparameter space
            "steps": 100,
            "width": tune.uniform(10, 100),
            "height": tune.uniform(0, 100),
        })
    print("Best hyperparameters found were: ", analysis.best_config)
