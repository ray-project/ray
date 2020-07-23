"""This test checks that Skopt is functional.

It also checks that it is usable with a separate scheduler.
"""
import time

import ray
from ray import tune
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.skopt import SkOptSearch


def evaluation_fn(step, width, height):
    return (0.1 + width * step / 100)**(-1) + height * 0.1


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config["steps"]):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back back to Tune.
        tune.report(iterations=step, mean_loss=intermediate_score)
        time.sleep(0.1)


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
            "steps": 100,
        }
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
    tune.run(
        easy_objective,
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
    tune.run(
        easy_objective,
        name="skopt_exp",
        search_alg=algo,
        scheduler=scheduler,
        **config)
