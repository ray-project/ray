"""This test checks that Skopt is functional.

It also checks that it is usable with a separate scheduler.
"""
import ray
from ray.tune import run
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.skopt import SkOptSearch


def easy_objective(config, reporter):
    import time
    time.sleep(0.2)
    for i in range(config["iterations"]):
        reporter(
            timesteps_total=i,
            mean_loss=(config["height"] - 14)**2 - abs(config["width"] - 3))
        time.sleep(0.02)


if __name__ == "__main__":
    import argparse
    from skopt import Optimizer

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
    optimizer = Optimizer([(0, 20), (-100, 100)])
    previously_run_params = [[10, 0], [15, -20]]
    known_rewards = [-189, -1144]
    algo = SkOptSearch(
        optimizer, ["width", "height"],
        metric="mean_loss",
        mode="min",
        points_to_evaluate=previously_run_params,
        evaluated_rewards=known_rewards)
    scheduler = AsyncHyperBandScheduler(metric="mean_loss", mode="min")
    run(easy_objective,
        name="skopt_exp_with_warmstart",
        search_alg=algo,
        scheduler=scheduler,
        **config)

    # Now run the experiment without known rewards

    algo = SkOptSearch(
        optimizer, ["width", "height"],
        metric="mean_loss",
        mode="min",
        points_to_evaluate=previously_run_params)
    scheduler = AsyncHyperBandScheduler(metric="mean_loss", mode="min")
    run(easy_objective,
        name="skopt_exp",
        search_alg=algo,
        scheduler=scheduler,
        **config)
