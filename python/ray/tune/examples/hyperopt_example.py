"""This test checks that HyperOpt is functional.

It also checks that it is usable with a separate scheduler.
"""
import ray
from ray.tune import run
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.hyperopt import HyperOptSearch


def easy_objective(config, reporter):
    import time
    time.sleep(0.2)
    assert type(config["activation"]) == str, \
        "Config is incorrect: {}".format(type(config["activation"]))
    for i in range(config["iterations"]):
        reporter(
            timesteps_total=i,
            mean_loss=(config["height"] - 14)**2 - abs(config["width"] - 3))
        time.sleep(0.02)


if __name__ == "__main__":
    import argparse
    from hyperopt import hp

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init(configure_logging=False)

    space = {
        "width": hp.uniform("width", 0, 20),
        "height": hp.uniform("height", -100, 100),
        "activation": hp.choice("activation", ["relu", "tanh"])
    }

    current_best_params = [
        {
            "width": 1,
            "height": 2,
            "activation": 0  # Activation will be relu
        },
        {
            "width": 4,
            "height": 2,
            "activation": 1  # Activation will be tanh
        }
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
    algo = HyperOptSearch(
        space,
        metric="mean_loss",
        mode="min",
        points_to_evaluate=current_best_params)
    scheduler = AsyncHyperBandScheduler(metric="mean_loss", mode="min")
    run(easy_objective, search_alg=algo, scheduler=scheduler, **config)
