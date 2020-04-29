"""This test checks that ZOOptSearch is functional.

It also checks that it is usable with a separate scheduler.
"""
import ray
from ray.tune import run
from ray.tune.suggest.zoopt import ZOOptSearch
from ray.tune.schedulers import AsyncHyperBandScheduler
from zoopt import ValueType


def easy_objective(config, reporter):
    import time
    time.sleep(0.2)
    for i in range(config["iterations"]):
        reporter(
            timesteps_total=i,
            mean_loss=(config["height"] - 14)**2 - abs(config["width"] - 3))
        time.sleep(0.02)


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
        "width": (ValueType.DISCRETE, [-10, 10], False)
    }

    config = {
        "num_samples": 10 if args.smoke_test else 1000,
        "config": {
            "iterations": 10,  # evaluation times
        },
        "stop": {
            "timesteps_total": 10  # cumstom stop rules
        }
    }

    zoopt_search = ZOOptSearch(
        algo="Asracos",  # only support ASRacos currently
        budget=config["num_samples"],
        dim_dict=dim_dict,
        metric="mean_loss",
        mode="min")

    scheduler = AsyncHyperBandScheduler(metric="mean_loss", mode="min")

    run(easy_objective,
        search_alg=zoopt_search,
        name="zoopt_search",
        scheduler=scheduler,
        **config)
