"""This test checks that Dragonfly is functional.

It also checks that it is usable with a separate scheduler.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.tune import run
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.dragonfly import DragonflySearch


def objective(config, reporter):
    import numpy as np
    import time
    time.sleep(0.2)
    for i in range(config["iterations"]):
        vol1 = config["point"][0]  # LiNO3
        vol2 = config["point"][1]  # Li2SO4
        vol3 = config["point"][2]  # NaClO4
        vol4 = 10 - (vol1 + vol2 + vol3)  # Water
        # Synthetic functions
        conductivity = vol1 + 0.1 * (vol2 + vol3)**2 + 2.3 * vol4 * (vol1**1.5)
        # Add Gaussian noise to simulate experimental noise
        conductivity += np.random.normal() * 0.01
        reporter(timesteps_total=i, objective=conductivity)
        time.sleep(0.02)


if __name__ == "__main__":
    import argparse
    from dragonfly.opt.gp_bandit import EuclideanGPBandit
    from dragonfly.exd.experiment_caller import EuclideanFunctionCaller
    from dragonfly import load_config

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
        },
    }

    domain_vars = [{
        "name": "LiNO3_vol",
        "type": "float",
        "min": 0,
        "max": 7
    }, {
        "name": "Li2SO4_vol",
        "type": "float",
        "min": 0,
        "max": 7
    }, {
        "name": "NaClO4_vol",
        "type": "float",
        "min": 0,
        "max": 7
    }]

    domain_config = load_config({"domain": domain_vars})

    func_caller = EuclideanFunctionCaller(
        None, domain_config.domain.list_of_domains[0])
    optimizer = EuclideanGPBandit(func_caller, ask_tell_mode=True)
    algo = DragonflySearch(optimizer, metric="objective", mode="max")
    scheduler = AsyncHyperBandScheduler(metric="objective", mode="max")
    run(objective,
        name="dragonfly_search",
        search_alg=algo,
        scheduler=scheduler,
        **config)
