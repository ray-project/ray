"""This test checks that Nevergrad is functional.

It also checks that it is usable with a separate scheduler.
"""
import time

import ray
from ray import tune
from ray.tune.suggest import ConcurrencyLimiter
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.nevergrad import NevergradSearch


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
    import nevergrad as ng

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init()

    # The config will be automatically converted to Nevergrad's search space
    tune_kwargs = {
        "num_samples": 10 if args.smoke_test else 50,
        "config": {
            "steps": 100,
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100),
            "activation": tune.choice(["relu", "tanh"])
        }
    }

    # Optional: Pass the parameter space yourself
    # space = ng.p.Dict(
    #     width=ng.p.Scalar(lower=0, upper=20),
    #     height=ng.p.Scalar(lower=-100, upper=100),
    #     activation=ng.p.Choice(choices=["relu", "tanh"])
    # )

    algo = NevergradSearch(
        optimizer=ng.optimizers.OnePlusOne,
        # space=space,  # If you want to set the space manually
    )
    algo = ConcurrencyLimiter(algo, max_concurrent=4)

    scheduler = AsyncHyperBandScheduler()

    tune.run(
        easy_objective,
        metric="mean_loss",
        mode="min",
        name="nevergrad",
        search_alg=algo,
        scheduler=scheduler,
        **tune_kwargs)
