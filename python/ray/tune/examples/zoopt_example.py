"""This test checks that ZOOptSearch is functional.

It also checks that it is usable with a separate scheduler.
"""
import time

import ray
from ray import tune
from ray.tune.suggest.zoopt import ZOOptSearch
from ray.tune.schedulers import AsyncHyperBandScheduler
from zoopt import ValueType


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

    # This dict could mix continuous dimensions and discrete dimensions,
    # for example:
    dim_dict = {
        # for continuous dimensions: (continuous, search_range, precision)
        "height": (ValueType.CONTINUOUS, [-10, 10], 1e-2),
        # for discrete dimensions: (discrete, search_range, has_order)
        "width": (ValueType.DISCRETE, [0, 10], False)
    }

    config = {
        "num_samples": 10 if args.smoke_test else 1000,
        "config": {
            "steps": 10,  # evaluation times
        }
    }

    zoopt_search = ZOOptSearch(
        algo="Asracos",  # only support ASRacos currently
        budget=config["num_samples"],
        dim_dict=dim_dict,
        metric="mean_loss",
        mode="min")

    scheduler = AsyncHyperBandScheduler(metric="mean_loss", mode="min")

    tune.run(
        easy_objective,
        search_alg=zoopt_search,
        name="zoopt_search",
        scheduler=scheduler,
        **config)
