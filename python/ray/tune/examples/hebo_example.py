"""This example demonstrates the usage of HEBO with Ray Tune.

It also checks that it is usable with a separate scheduler.
"""
import time

from ray import tune
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.hebo import HEBOSearch


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
        tune.report(iterations=step, mean_loss=intermediate_score)


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

    # The config will be automatically converted to HEBO's DesignSpace

    # Optional: Pass the parameter space yourself
    #
    # from hebo.design_space.design_space import DesignSpace
    # space_cfg = [
    #     {
    #         "name": "width",
    #         "type": "num",
    #         "lb": 0,
    #         "ub": 20
    #     },
    #     {
    #         "name": "height",
    #         "type": "num",
    #         "lb": -100,
    #         "ub": 100
    #     },
    #     {
    #         "name": "activation",
    #         "type": "cat",
    #         "categories": ["relu", "tanh"]
    #     },
    # ]
    # space = DesignSpace().parse(space_cfg)

    previously_run_params = [
        {"width": 10, "height": 0, "activation": "relu"},  # Activation will be relu
        {"width": 15, "height": -20, "activation": "tanh"},  # Activation will be tanh
    ]
    known_rewards = [-189, -1144]

    # maximum number of concurrent trials
    max_concurrent = 8

    algo = HEBOSearch(
        # space = space, # If you want to set the space
        points_to_evaluate=previously_run_params,
        evaluated_rewards=known_rewards,
        random_state_seed=123,  # for reproducibility
        max_concurrent=max_concurrent,
    )

    scheduler = AsyncHyperBandScheduler()

    analysis = tune.run(
        easy_objective,
        metric="mean_loss",
        mode="min",
        name="hebo_exp_with_warmstart",
        search_alg=algo,
        scheduler=scheduler,
        num_samples=10 if args.smoke_test else 50,
        config={
            "steps": 100,
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100),
            "activation": tune.choice(["relu", "tanh"]),
        },
    )
    print("Best hyperparameters found were: ", analysis.best_config)
