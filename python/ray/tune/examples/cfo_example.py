"""This example demonstrates the usage of CFO with Ray Tune.

It also checks that it is usable with a separate scheduler.

Requires the FLAML library to be installed (`pip install flaml`).
"""
import time

import ray
from ray import train, tune
from ray.tune.search import ConcurrencyLimiter
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.search.flaml import CFO


def evaluation_fn(step, width, height):
    return (0.1 + width * step / 100) ** (-1) + height * 0.1


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config["steps"]):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back back to Tune.
        train.report({"iterations": step, "mean_loss": intermediate_score})
        time.sleep(0.1)


def run_cfo_tune(smoke_test=False):
    algo = CFO()
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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

    ray.init(configure_logging=False)

    run_cfo_tune(smoke_test=args.smoke_test)
