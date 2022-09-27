"""This example demonstrates the usage of SigOpt with Ray Tune.

It also checks that it is usable with a separate scheduler.

Requires the SigOpt library to be installed (`pip install sigopt`).
"""
import sys
import time

from ray import air, tune
from ray.air import session
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.search.sigopt import SigOptSearch
from ray.tune.search.sigopt.sigopt_search import load_sigopt_key


def evaluate(step, width, height):
    return (0.1 + width * step / 100) ** (-1) + height * 0.01


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config["steps"]):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluate(step, width, height)
        # Feed the score back back to Tune.
        session.report({"iterations": step, "mean_loss": intermediate_score})
        time.sleep(0.1)


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

    load_sigopt_key()

    if "SIGOPT_KEY" not in os.environ:
        if args.smoke_test:
            print("SigOpt API Key not found. Skipping smoke test.")
            sys.exit(0)
        else:
            raise ValueError(
                "SigOpt API Key not found. Please set the SIGOPT_KEY "
                "environment variable."
            )

    space = [
        {
            "name": "width",
            "type": "int",
            "bounds": {"min": 0, "max": 20},
        },
        {
            "name": "height",
            "type": "int",
            "bounds": {"min": -100, "max": 100},
        },
    ]
    algo = SigOptSearch(
        space,
        name="SigOpt Example Experiment",
        metric="mean_loss",
        mode="min",
    )
    scheduler = AsyncHyperBandScheduler(metric="mean_loss", mode="min")
    tuner = tune.Tuner(
        easy_objective,
        run_config=air.RunConfig(
            name="my_exp",
        ),
        tune_config=tune.TuneConfig(
            search_alg=algo,
            scheduler=scheduler,
            num_samples=4 if args.smoke_test else 100,
        ),
        param_space={"steps": 10},
    )
    results = tuner.fit()

    print(
        "Best hyperparameters found were: ",
        results.get_best_result("mean_loss", "min").config,
    )
