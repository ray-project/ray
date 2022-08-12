"""This example demonstrates the usage of SkOpt with Ray Tune.

It also checks that it is usable with a separate scheduler.

Requires the SkOpt library to be installed (`pip install scikit-optimize`).
"""
import time

from ray import air, tune
from ray.air import session
from ray.tune.search import ConcurrencyLimiter
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.search.skopt import SkOptSearch


def evaluation_fn(step, width, height):
    time.sleep(0.1)
    return (0.1 + width * step / 100) ** (-1) + height * 0.1


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config["steps"]):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back back to Tune.
        session.report({"iterations": step, "mean_loss": intermediate_score})


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

    # The config will be automatically converted to SkOpt's search space

    # Optional: Pass the parameter space yourself
    # space = {
    #     "width": (0, 20),
    #     "height": (-100, 100),
    #     "activation": ["relu", "tanh"]
    # }

    previously_run_params = [
        {"width": 10, "height": 0, "activation": "relu"},  # Activation will be relu
        {"width": 15, "height": -20, "activation": "tanh"},  # Activation will be tanh
    ]
    known_rewards = [-189, -1144]

    algo = SkOptSearch(
        # parameter_names=space.keys(),  # If you want to set the space
        # parameter_ranges=space.values(), # If you want to set the space
        points_to_evaluate=previously_run_params,
        evaluated_rewards=known_rewards,
    )
    algo = ConcurrencyLimiter(algo, max_concurrent=4)

    scheduler = AsyncHyperBandScheduler()

    tuner = tune.Tuner(
        easy_objective,
        tune_config=tune.TuneConfig(
            metric="mean_loss",
            mode="min",
            search_alg=algo,
            scheduler=scheduler,
            num_samples=10 if args.smoke_test else 50,
        ),
        run_config=air.RunConfig(
            name="skopt_exp_with_warmstart",
        ),
        param_space={
            "steps": 100,
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100),
            "activation": tune.choice(["relu", "tanh"]),
        },
    )
    results = tuner.fit()
    print("Best hyperparameters found were: ", results.get_best_result().config)
