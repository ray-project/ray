"""This test checks that the Trial Runner is functional.

Specifically, it checks that the logging mechanisms are
functional and accurate.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.tune import run_experiments, register_trainable


def easy_objective(config, reporter):
    for i in range(config["iterations"]):
        reporter(itr=i, done=i == 99)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init(redirect_output=True)

    register_trainable("exp", easy_objective)

    config = {
        "my_exp": {
            "run": "exp",
            "config": {
                "iterations": 100,
            },
            "stop": {
                "timesteps_total": 100
            },
        }
    }
    run_experiments(config)
