"""This example demonstrates the usage of BlendSearch with Ray Tune.

It also checks that it is usable with a separate scheduler.

Requires the FLAML library to be installed (`pip install flaml`).
"""
import time

import ray
from ray import train, tune
from ray.tune.search import ConcurrencyLimiter
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.search.flaml import BlendSearch


def evaluation_fn(step, width, height):
    return (0.1 + width * step / 100) ** (-1) + height * 0.1


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config["steps"]):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back to Tune.
        train.report({"iterations": step, "mean_loss": intermediate_score})
        time.sleep(0.1)


def run_blendsearch_tune(smoke_test=False):
    algo = BlendSearch()
    algo = ConcurrencyLimiter(algo, max_concurrent=4)
    scheduler = AsyncHyperBandScheduler()
    tuner = tune.Tuner(
        easy_objective,
        tune_config=tune.TuneConfig(
            metric="mean_loss",
            mode="min",
            search_alg=algo,
            scheduler=scheduler,
            num_samples=10 if smoke_test else 100,
        ),
        param_space={
            "steps": 100,
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100),
            # This is an ignored parameter.
            "activation": tune.choice(["relu", "tanh"]),
        },
    )
    results = tuner.fit()

    print("Best hyperparameters found were: ", results.get_best_result().config)


def run_blendsearch_tune_w_budget(time_budget_s=10):
    """run BlendSearch with given time_budget_s"""
    algo = BlendSearch(
        metric="mean_loss",
        mode="min",
        space={
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100),
            "activation": tune.choice(["relu", "tanh"]),
        },
    )
    algo.set_search_properties(config={"time_budget_s": time_budget_s})
    algo = ConcurrencyLimiter(algo, max_concurrent=4)
    scheduler = AsyncHyperBandScheduler()
    tuner = tune.Tuner(
        easy_objective,
        tune_config=tune.TuneConfig(
            metric="mean_loss",
            mode="min",
            search_alg=algo,
            scheduler=scheduler,
            time_budget_s=time_budget_s,
            num_samples=-1,
        ),
        param_space={
            "steps": 100,
        },
    )
    results = tuner.fit()

    print("Best hyperparameters found were: ", results.get_best_result().config)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

    ray.init(configure_logging=False)

    run_blendsearch_tune_w_budget(time_budget_s=30)
    run_blendsearch_tune(smoke_test=args.smoke_test)
