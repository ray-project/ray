#!/usr/bin/env python

import argparse
import time

import ray
from ray import air, tune
from ray.air import session
from ray.tune.schedulers import AsyncHyperBandScheduler


def evaluation_fn(step, width, height):
    time.sleep(0.1)
    return (0.1 + width * step / 100) ** (-1) + height * 0.1


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config["steps"]):
        # Iterative training function - can be an arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back back to Tune.
        session.report({"iterations": step, "mean_loss": intermediate_score})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    parser.add_argument(
        "--ray-address",
        help="Address of Ray cluster for seamless distributed execution.",
        required=False,
    )
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using Ray Client.",
    )
    args, _ = parser.parse_known_args()
    if args.server_address is not None:
        ray.init(f"ray://{args.server_address}")
    else:
        ray.init(address=args.ray_address)

    # AsyncHyperBand enables aggressive early stopping of bad trials.
    scheduler = AsyncHyperBandScheduler(grace_period=5, max_t=100)

    # 'training_iteration' is incremented every time `trainable.step` is called
    stopping_criteria = {"training_iteration": 1 if args.smoke_test else 9999}

    tuner = tune.Tuner(
        tune.with_resources(easy_objective, {"cpu": 1, "gpu": 0}),
        run_config=air.RunConfig(
            name="asynchyperband_test",
            stop=stopping_criteria,
            verbose=1,
        ),
        tune_config=tune.TuneConfig(
            metric="mean_loss", mode="min", scheduler=scheduler, num_samples=20
        ),
        param_space={  # Hyperparameter space
            "steps": 100,
            "width": tune.uniform(10, 100),
            "height": tune.uniform(0, 100),
        },
    )
    results = tuner.fit()
    print("Best hyperparameters found were: ", results.get_best_result().config)
