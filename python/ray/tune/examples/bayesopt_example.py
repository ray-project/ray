"""This test checks that BayesOpt is functional.

It also checks that it is usable with a separate scheduler.
"""
import time

import ray
from ray import tune
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.bayesopt import BayesOptSearch


def evaluation_fn(step, width, height):
    return (0.1 + width * step / 100)**(-1) + height * 0.1


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config["steps"]):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back back to Tune.
        tune.report(iterations=step, mean_loss=intermediate_score)
        time.sleep(0.1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init()

    space = {"width": (0, 20), "height": (-100, 100)}

    config = {
        "num_samples": 10 if args.smoke_test else 1000,
        "config": {
            "steps": 100,
        }
    }
    algo = BayesOptSearch(
        space,
        metric="mean_loss",
        mode="min",
        utility_kwargs={
            "kind": "ucb",
            "kappa": 2.5,
            "xi": 0.0
        })
    scheduler = AsyncHyperBandScheduler(metric="mean_loss", mode="min")
    tune.run(
        easy_objective,
        name="my_exp",
        search_alg=algo,
        scheduler=scheduler,
        **config)
