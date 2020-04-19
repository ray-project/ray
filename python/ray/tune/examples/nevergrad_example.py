"""This test checks that Nevergrad is functional.

It also checks that it is usable with a separate scheduler.
"""
import ray
from ray.tune import run
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.nevergrad import NevergradSearch


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
    from nevergrad.optimization import optimizerlib

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
    run(easy_objective,
        name="nevergrad",
        search_alg=algo,
        scheduler=scheduler,
        **config)
