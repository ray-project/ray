"""This example demonstrates basic Ray Tune random search and grid search."""
import time

import ray
from ray.air import session
from ray import tune


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
    if args.server_address is not None:
        ray.init(f"ray://{args.server_address}")
    else:
        ray.init(configure_logging=False)

    # This will do a grid search over the `activation` parameter. This means
    # that each of the two values (`relu` and `tanh`) will be sampled once
    # for each sample (`num_samples`). We end up with 2 * 50 = 100 samples.
    # The `width` and `height` parameters are sampled randomly.
    # `steps` is a constant parameter.

    tuner = tune.Tuner(
        easy_objective,
        tune_config=tune.TuneConfig(
            metric="mean_loss",
            mode="min",
            num_samples=5 if args.smoke_test else 50,
        ),
        param_space={
            "steps": 5 if args.smoke_test else 100,
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100),
            "activation": tune.grid_search(["relu", "tanh"]),
        },
    )
    results = tuner.fit()

    print("Best hyperparameters found were: ", results.get_best_result().config)
