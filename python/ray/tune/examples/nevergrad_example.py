"""This test checks that Nevergrad is functional.

It also checks that it is usable with a separate scheduler.
"""
import time

import ray
from ray import tune
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
    from nevergrad.optimization import optimizerlib

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
    instrumentation = 2
    parameter_names = ["height", "width"]
    # With nevergrad v0.2.0+ the following is also possible:
    # from nevergrad import instrumentation as inst
    # instrumentation = inst.Instrumentation(
    #     height=inst.var.Array(1).bounded(0, 200).asfloat(),
    #     width=inst.var.OrderedDiscrete([0, 10, 20, 30, 40, 50]))
    # parameter_names = None  # names are provided by the instrumentation
    optimizer = optimizerlib.OnePlusOne(instrumentation)
    algo = NevergradSearch(
        optimizer, parameter_names, metric="mean_loss", mode="min")
    scheduler = AsyncHyperBandScheduler(metric="mean_loss", mode="min")
    tune.run(
        easy_objective,
        name="nevergrad",
        search_alg=algo,
        scheduler=scheduler,
        **config)
