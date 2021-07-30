"""This example demonstrates the usage of Optuna with Ray Tune.

It also checks that it is usable with a separate scheduler.
"""
import time

import ray
from ray import tune
from ray.tune.suggest import ConcurrencyLimiter
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.optuna import OptunaSearch


def evaluation_fn(step, width, height, mult=1):
    return (0.1 + width * step / 100)**(-1) + height * 0.1 * mult


def easy_objective(config):
    # Hyperparameters
    width, height, mult = config["width"], config["height"], config.get(
        "mult", 1)
    print(config)

    for step in range(config["steps"]):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height, mult)
        # Feed the score back back to Tune.
        tune.report(iterations=step, mean_loss=intermediate_score)
        time.sleep(0.1)


def objective(trial):
    activation = trial.suggest_categorical("activation", ["relu", "tanh"])
    trial.suggest_float("width", 0, 20)
    trial.suggest_float("height", -100, 100)
    if activation == "relu":
        trial.suggest_float("mult", 1, 2)

    # return all constants in a dictionary
    return {"steps": 100}


def run_optuna_tune(smoke_test=False):
    algo = OptunaSearch(space=objective, metric="mean_loss", mode="min")
    algo = ConcurrencyLimiter(algo, max_concurrent=4)
    scheduler = AsyncHyperBandScheduler()
    analysis = tune.run(
        easy_objective,
        metric="mean_loss",
        mode="min",
        search_alg=algo,
        scheduler=scheduler,
        num_samples=10 if smoke_test else 100,
    )

    print("Best hyperparameters found were: ", analysis.best_config)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using "
        "Ray Client.")
    args, _ = parser.parse_known_args()
    if args.server_address is not None:
        ray.util.connect(args.server_address)
    else:
        ray.init(configure_logging=False)

    run_optuna_tune(smoke_test=args.smoke_test)
