"""This test checks that SigOpt is functional.

It also checks that it is usable with a separate scheduler.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.tune import run
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest import SigOptSearch


def easy_objective(config, reporter):
    import time
    time.sleep(0.2)
    for i in range(config["iterations"]):
        reporter(
            timesteps_total=i,
            neg_mean_loss=-(config["height"] - 14)**2 +
            abs(config["width"] - 3))
        time.sleep(0.02)


if __name__ == "__main__":
    import argparse
    import os

    assert "SIGOPT_KEY" in os.environ, \
        "SigOpt API key must be stored as environment variable at SIGOPT_KEY"

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init()

    space = [
        {
            'name': 'width',
            'type': 'int',
            'bounds': {
                'min': 0,
                'max': 20
            },
        },
        {
            'name': 'height',
            'type': 'int',
            'bounds': {
                'min': -100,
                'max': 100
            },
        },
    ]

    config = {
        "num_samples": 10 if args.smoke_test else 1000,
        "config": {
            "iterations": 100,
        },
        "stop": {
            "timesteps_total": 100
        },
    }
    algo = SigOptSearch(
        space,
        name="SigOpt Example Experiment",
        max_concurrent=1,
        reward_attr="neg_mean_loss")
    scheduler = AsyncHyperBandScheduler(reward_attr="neg_mean_loss")
    run(easy_objective,
        name="my_exp",
        search_alg=algo,
        scheduler=scheduler,
        **config)
