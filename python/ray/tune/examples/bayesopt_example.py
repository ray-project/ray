"""This example demonstrates the usage of BayesOpt with Ray Tune.

It also checks that it is usable with a separate scheduler.

Requires the BayesOpt library to be installed (`pip install bayesian-optimization`).
"""
import time

from ray import tune
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.search import ConcurrencyLimiter
from ray.tune.search.bayesopt import BayesOptSearch


def evaluation_fn(step, width, height):
    return (0.1 + width * step / 100) ** (-1) + height * 0.1


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

    if args.server_address:
        import ray

        ray.init(f"ray://{args.server_address}")

    algo = BayesOptSearch(utility_kwargs={"kind": "ucb", "kappa": 2.5, "xi": 0.0})
    algo = ConcurrencyLimiter(algo, max_concurrent=4)
    scheduler = AsyncHyperBandScheduler()
    analysis = tune.run(
        easy_objective,
        name="my_exp",
        metric="mean_loss",
        mode="min",
        search_alg=algo,
        scheduler=scheduler,
        num_samples=10 if args.smoke_test else 1000,
        config={
            "steps": 100,
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100),
        },
    )

    print("Best hyperparameters found were: ", analysis.best_config)
