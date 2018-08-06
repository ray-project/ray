"""This test checks that HyperOpt is functional.

It also checks that it is usable with a separate scheduler.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.tune import run_experiments, register_trainable
from ray.tune.async_hyperband import AsyncHyperBandScheduler
from ray.tune.suggest import HyperOptSearch


def easy_objective(config, reporter):
    import time
    time.sleep(0.2)
    assert type(config["activation"]) == str, \
        "Config is incorrect: {}".format(type(config["activation"]))
    for i in range(100):
        reporter(
            timesteps_total=i,
            neg_mean_loss=-(config["height"] - 14)**2 +
            abs(config["width"] - 3))
        time.sleep(0.02)


if __name__ == '__main__':
    import argparse
    from hyperopt import hp

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init(redirect_output=True)

    register_trainable("exp", easy_objective)

    space = {
        'width': hp.uniform('width', 0, 20),
        'height': hp.uniform('height', -100, 100),
        'activation': hp.choice("activation", ["relu", "tanh"])
    }

    config = {
        "my_exp": {
            "run": "exp",
            "repeat": 10 if args.smoke_test else 1000,
            "stop": {
                "training_iteration": 100
            },
        }
    }
    algo = HyperOptSearch(
        config, space, max_concurrent=4, reward_attr="neg_mean_loss")
    scheduler = AsyncHyperBandScheduler(reward_attr="neg_mean_loss")
    run_experiments(search_alg=algo, scheduler=scheduler)
