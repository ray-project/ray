"""This test checks that SigOpt is functional.

It also checks that it is usable with a separate scheduler.
"""
import time

import ray
from ray import tune
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.sigopt import SigOptSearch


def evaluate(step, width, height):
    return (0.1 + width * step / 100)**(-1) + height * 0.01


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config["steps"]):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluate(step, width, height)
        # Feed the score back back to Tune.
        tune.report(iterations=step, mean_loss=intermediate_score)
        time.sleep(0.1)


if __name__ == "__main__":
    import argparse
    import os

    assert "SIGOPT_KEY" in os.environ, \
        "SigOpt API key must be stored as environment variable at SIGOPT_KEY"

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init()

    space = [
        {
            "name": "width",
            "type": "int",
            "bounds": {
                "min": 0,
                "max": 20
            },
        },
        {
            "name": "height",
            "type": "int",
            "bounds": {
                "min": -100,
                "max": 100
            },
        },
    ]

    config = {
        "num_samples": 10 if args.smoke_test else 1000,
        "config": {
            "steps": 10
        }
    }
    algo = SigOptSearch(
        space,
        name="SigOpt Example Experiment",
        max_concurrent=1,
        metric="mean_loss",
        mode="min")
    scheduler = AsyncHyperBandScheduler(metric="mean_loss", mode="min")
    tune.run(
        easy_objective,
        name="my_exp",
        search_alg=algo,
        scheduler=scheduler,
        **config)
