""""Example using Sigopt's support for prior beliefs."""
import sys

import numpy as np
from ray import tune

from ray.tune.suggest.sigopt import SigOptSearch

np.random.seed(0)
vector1 = np.random.normal(0.0, 0.1, 100)
vector2 = np.random.normal(0.0, 0.1, 100)
vector3 = np.random.normal(0.0, 0.1, 100)


def evaluate(w1, w2, w3):
    total = w1 * vector1 + w2 * vector2 + w3 * vector3
    return total.mean(), total.std()


def easy_objective(config):
    # Hyperparameters
    w1 = config["w1"]
    w2 = config["w2"]
    total = (w1 + w2)
    if total > 1:
        w3 = 0
        w1 /= total
        w2 /= total
    else:
        w3 = 1 - total

    average, std = evaluate(w1, w2, w3)
    tune.report(average=average, std=std)


if __name__ == "__main__":
    import argparse
    import os
    from sigopt import Connection

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()

    if "SIGOPT_KEY" not in os.environ:
        if args.smoke_test:
            print("SigOpt API Key not found. Skipping smoke test.")
            sys.exit(0)
        else:
            raise ValueError(
                "SigOpt API Key not found. Please set the SIGOPT_KEY "
                "environment variable.")

    samples = 4 if args.smoke_test else 100

    conn = Connection(client_token=os.environ["SIGOPT_KEY"])
    experiment = conn.experiments().create(
        name="prior experiment example",
        parameters=[{
            "name": "w1",
            "bounds": {
                "max": 1,
                "min": 0
            },
            "prior": {
                "mean": 1 / 3,
                "name": "normal",
                "scale": 0.2
            },
            "type": "double"
        }, {
            "name": "w2",
            "bounds": {
                "max": 1,
                "min": 0
            },
            "prior": {
                "mean": 1 / 3,
                "name": "normal",
                "scale": 0.2
            },
            "type": "double"
        }],
        metrics=[
            dict(name="std", objective="minimize", strategy="optimize"),
            dict(name="average", strategy="store")
        ],
        observation_budget=samples,
        parallel_bandwidth=1)

    algo = SigOptSearch(
        connection=conn,
        experiment_id=experiment.id,
        name="SigOpt Example Existing Experiment",
        max_concurrent=1,
        metric=["average", "std"],
        mode=["obs", "min"])

    analysis = tune.run(
        easy_objective,
        name="my_exp",
        search_alg=algo,
        num_samples=samples,
        config={})

    print("Best hyperparameters found were: ",
          analysis.get_best_config("average", "min"))
