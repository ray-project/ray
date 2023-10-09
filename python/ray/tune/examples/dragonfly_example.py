"""This example demonstrates the usage of Dragonfly with Ray Tune.

It also checks that it is usable with a separate scheduler.

Requires the Dragonfly library to be installed (`pip install dragonfly-opt`).
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import time

from ray import train, tune
from ray.tune.search import ConcurrencyLimiter
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.search.dragonfly import DragonflySearch


def objective(config):
    for i in range(config["iterations"]):
        vol1 = config["LiNO3_vol"]  # LiNO3
        vol2 = config["Li2SO4_vol"]  # Li2SO4
        vol3 = config["NaClO4_vol"]  # NaClO4
        vol4 = 10 - (vol1 + vol2 + vol3)  # Water
        # Synthetic functions
        conductivity = vol1 + 0.1 * (vol2 + vol3) ** 2 + 2.3 * vol4 * (vol1**1.5)
        # Add Gaussian noise to simulate experimental noise
        conductivity += np.random.normal() * 0.01
        train.report({"timesteps_total": i, "objective": conductivity})
        time.sleep(0.02)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

    # Optional: Pass the parameter space yourself
    # space = [{
    #     "name": "LiNO3_vol",
    #     "type": "float",
    #     "min": 0,
    #     "max": 7
    # }, {
    #     "name": "Li2SO4_vol",
    #     "type": "float",
    #     "min": 0,
    #     "max": 7
    # }, {
    #     "name": "NaClO4_vol",
    #     "type": "float",
    #     "min": 0,
    #     "max": 7
    # }]

    df_search = DragonflySearch(
        optimizer="bandit",
        domain="euclidean",
        # space=space,  # If you want to set the space manually
    )
    df_search = ConcurrencyLimiter(df_search, max_concurrent=4)

    scheduler = AsyncHyperBandScheduler()
    tuner = tune.Tuner(
        objective,
        tune_config=tune.TuneConfig(
            metric="objective",
            mode="max",
            search_alg=df_search,
            scheduler=scheduler,
            num_samples=10 if args.smoke_test else 50,
        ),
        run_config=train.RunConfig(
            name="dragonfly_search",
        ),
        param_space={
            "iterations": 100,
            "LiNO3_vol": tune.uniform(0, 7),
            "Li2SO4_vol": tune.uniform(0, 7),
            "NaClO4_vol": tune.uniform(0, 7),
        },
    )
    results = tuner.fit()

    print("Best hyperparameters found were: ", results.get_best_result().config)
