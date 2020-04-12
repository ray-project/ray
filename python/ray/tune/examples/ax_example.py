"""This test checks that AxSearch is functional.

It also checks that it is usable with a separate scheduler.
"""
import numpy as np

import ray
from ray.tune import run
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.ax import AxSearch


def hartmann6(x):
    alpha = np.array([1.0, 1.2, 3.0, 3.2])
    A = np.array([
        [10, 3, 17, 3.5, 1.7, 8],
        [0.05, 10, 17, 0.1, 8, 14],
        [3, 3.5, 1.7, 10, 17, 8],
        [17, 8, 0.05, 10, 0.1, 14],
    ])
    P = 10**(-4) * np.array([
        [1312, 1696, 5569, 124, 8283, 5886],
        [2329, 4135, 8307, 3736, 1004, 9991],
        [2348, 1451, 3522, 2883, 3047, 6650],
        [4047, 8828, 8732, 5743, 1091, 381],
    ])
    y = 0.0
    for j, alpha_j in enumerate(alpha):
        t = 0
        for k in range(6):
            t += A[j, k] * ((x[k] - P[j, k])**2)
        y -= alpha_j * np.exp(-t)
    return y


def easy_objective(config, reporter):
    import time
    time.sleep(0.2)
    for i in range(config["iterations"]):
        x = np.array([config.get("x{}".format(i + 1)) for i in range(6)])
        reporter(
            timesteps_total=i,
            hartmann6=hartmann6(x),
            l2norm=np.sqrt((x**2).sum()))
        time.sleep(0.02)


if __name__ == "__main__":
    import argparse
    from ax.service.ax_client import AxClient

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()

    ray.init()

    config = {
        "num_samples": 10 if args.smoke_test else 50,
        "config": {
            "iterations": 100,
        },
        "stop": {
            "timesteps_total": 100
        }
    }
    parameters = [
        {
            "name": "x1",
            "type": "range",
            "bounds": [0.0, 1.0],
            "value_type": "float",  # Optional, defaults to "bounds".
            "log_scale": False,  # Optional, defaults to False.
        },
        {
            "name": "x2",
            "type": "range",
            "bounds": [0.0, 1.0],
        },
        {
            "name": "x3",
            "type": "range",
            "bounds": [0.0, 1.0],
        },
        {
            "name": "x4",
            "type": "range",
            "bounds": [0.0, 1.0],
        },
        {
            "name": "x5",
            "type": "range",
            "bounds": [0.0, 1.0],
        },
        {
            "name": "x6",
            "type": "range",
            "bounds": [0.0, 1.0],
        },
    ]
    client = AxClient(enforce_sequential_optimization=False)
    client.create_experiment(
        parameters=parameters,
        objective_name="hartmann6",
        minimize=True,  # Optional, defaults to False.
        parameter_constraints=["x1 + x2 <= 2.0"],  # Optional.
        outcome_constraints=["l2norm <= 1.25"],  # Optional.
    )
    algo = AxSearch(client, max_concurrent=4)
    scheduler = AsyncHyperBandScheduler(metric="hartmann6", mode="min")
    run(easy_objective,
        name="ax",
        search_alg=algo,
        scheduler=scheduler,
        **config)
