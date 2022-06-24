"""
Example using Sigopt's multi-objective functionality.

Requires the SigOpt library to be installed (`pip install sigopt`).
"""
import sys
import time

import numpy as np
from ray import tune
from ray.tune.search.sigopt import SigOptSearch

np.random.seed(0)
vector1 = np.random.normal(0, 0.1, 100)
vector2 = np.random.normal(0, 0.1, 100)


def evaluate(w1, w2):
    total = w1 * vector1 + w2 * vector2
    return total.mean(), total.std()


def easy_objective(config):
    # Hyperparameters
    w1 = config["w1"]
    w2 = config["total_weight"] - w1

    average, std = evaluate(w1, w2)
    tune.report(average=average, std=std, sharpe=average / std)
    time.sleep(0.1)


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

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
            "name": "w1",
            "type": "double",
            "bounds": {"min": 0, "max": 1},
        },
    ]

    algo = SigOptSearch(
        space,
        name="SigOpt Example Multi Objective Experiment",
        observation_budget=4 if args.smoke_test else 100,
        metric=["average", "std", "sharpe"],
        mode=["max", "min", "obs"],
    )

    analysis = tune.run(
        easy_objective,
        name="my_exp",
        search_alg=algo,
        num_samples=4 if args.smoke_test else 100,
        config={"total_weight": 1},
    )
    print(
        "Best hyperparameters found were: ", analysis.get_best_config("average", "min")
    )
