"""This example demonstrates the usage of conditional search spaces with Tune.

It also checks that it is usable with a separate scheduler.

For an example of using a Tune search space, see
:doc:`/tune/examples/hyperopt_example`.
"""
import time

import ray
from ray import tune
from ray.tune.suggest import ConcurrencyLimiter
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.hyperopt import HyperOptSearch
from hyperopt import hp


def f_unpack_dict(dct):
    """
    Unpacks all sub-dictionaries in given dictionary recursively.
    There should be no duplicated keys across all nested
    subdictionaries, or some instances will be lost without warning

    Source: https://www.kaggle.com/fanvacoolt/tutorial-on-hyperopt

    Parameters:
    ----------------
    dct : dictionary to unpack

    Returns:
    ----------------
    : unpacked dictionary
    """

    res = {}
    for (k, v) in dct.items():
        if isinstance(v, dict):
            res = {**res, **f_unpack_dict(v)}
        else:
            res[k] = v

    return res


def evaluation_fn(step, width, height, mult=1):
    return (0.1 + width * step / 100)**(-1) + height * 0.1 * mult


def easy_objective(config_in):
    # Hyperparameters
    config = f_unpack_dict(config_in)
    width, height, mult = config["width"], config["height"], config.get(
        "mult", 1)
    print(config)

    for step in range(config["steps"]):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height, mult)
        # Feed the score back back to Tune.
        tune.report(iterations=step, mean_loss=intermediate_score)
        time.sleep(0.1)


config_space = {
    "activation": hp.choice("activation", [
        {
            "activation": "relu",
            "mult": hp.uniform("mult", 1, 2)
        },
        {
            "activation": "tanh"
        },
    ]),
    "width": hp.uniform("width", 0, 20),
    "height": hp.uniform("heright", -100, 100),
    "steps": 100
}


def run_hyperopt_tune(config_dict=config_space, smoke_test=False):
    algo = HyperOptSearch(space=config_dict, metric="mean_loss", mode="min")
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

    run_hyperopt_tune(smoke_test=args.smoke_test)
