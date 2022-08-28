"""This example demonstrates the usage of Optuna with Ray Tune for
multi-objective optimization.

Please note that schedulers may not work correctly with multi-objective
optimization.

Requires the Optuna library to be installed (`pip install optuna`).
"""
import time

import ray
from ray import tune
from ray.air import session
from ray.tune.search import ConcurrencyLimiter
from ray.tune.search.optuna import OptunaSearch


def evaluation_fn(step, width, height):
    return (0.1 + width * step / 100) ** (-1) + height * 0.1


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config["steps"]):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back back to Tune.
        session.report(
            {
                "iterations": step,
                "loss": intermediate_score,
                "gain": intermediate_score * width,
            }
        )
        time.sleep(0.1)


def run_optuna_tune(smoke_test=False):
    algo = OptunaSearch(metric=["loss", "gain"], mode=["min", "max"])
    algo = ConcurrencyLimiter(algo, max_concurrent=4)
    tuner = tune.Tuner(
        easy_objective,
        tune_config=tune.TuneConfig(
            search_alg=algo,
            num_samples=10 if smoke_test else 100,
        ),
        param_space={
            "steps": 100,
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100),
            # This is an ignored parameter.
            "activation": tune.choice(["relu", "tanh"]),
        },
    )
    results = tuner.fit()

    print(
        "Best hyperparameters for loss found were: ",
        results.get_best_result("loss", "min").config,
    )
    print(
        "Best hyperparameters for gain found were: ",
        results.get_best_result("gain", "max").config,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
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
        ray.init(configure_logging=False)

    run_optuna_tune(smoke_test=args.smoke_test)
