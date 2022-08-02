"""This example demonstrates the usage of ZOOptSearch.

It also checks that it is usable with a separate scheduler.

Requires the ZOOpt library to be installed (`pip install zoopt`).
"""
import time

from ray import air, tune
from ray.air import session
from ray.tune.search.zoopt import ZOOptSearch
from ray.tune.schedulers import AsyncHyperBandScheduler


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

    num_samples = 10 if args.smoke_test else 1000

    # Optional: Pass the parameter space yourself
    # from zoopt import ValueType
    # space = {
    #     # for continuous dimensions: (continuous, search_range, precision)
    #     "height": (ValueType.CONTINUOUS, [-10, 10], 1e-2),
    #     # for discrete dimensions: (discrete, search_range, has_order)
    #     "width": (ValueType.DISCRETE, [0, 10], True)
    #     # for grid dimensions: (grid, grid_list)
    #     "layers": (ValueType.GRID, [4, 8, 16])
    # }

    zoopt_search_config = {
        "parallel_num": 8,
    }

    zoopt_search = ZOOptSearch(
        algo="Asracos",  # only support ASRacos currently
        budget=num_samples,
        # dim_dict=space,  # If you want to set the space yourself
        **zoopt_search_config,
    )

    scheduler = AsyncHyperBandScheduler()

    tuner = tune.Tuner(
        easy_objective,
        tune_config=tune.TuneConfig(
            metric="mean_loss",
            mode="min",
            search_alg=zoopt_search,
            scheduler=scheduler,
            num_samples=num_samples,
        ),
        run_config=air.RunConfig(
            name="zoopt_search",
        ),
        param_space={
            "steps": 10,
            "height": tune.quniform(-10, 10, 1e-2),
            "width": tune.randint(0, 10),
        },
    )
    results = tuner.fit()
    print("Best config found: ", results.get_best_result().config)
