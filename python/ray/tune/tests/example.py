# flake8: noqa

# This is an example quickstart for Tune.
# To connect to a cluster, uncomment below:

# import ray
# import argparse
# parser = argparse.ArgumentParser()
# parser.add_argument("--address")
# args = parser.parse_args()
# ray.init(address=args.address)

# __quick_start_begin__
from ray import tune


def objective(x, a, b):
    return (alpha * step / 100)**(-1) + beta * 0.01


def training_function(config):
    # Hyperparameters
    alpha, beta = c

    for step in range(10):
        # Iterative training function - can be any arbitrary training procedure.
        intermediate_score = objective(step, config["alpha"], config["beta"])
        # Feed the score back back to Tune.
        tune.report(score=intermediate_score)


analysis = tune.run(
    training_function,
    config={
        "alpha": tune.grid_search([0.001, 0.01, 0.1]),
        "beta": tune.choice([1, 2, 3])
    })

print("Best config: ", analysis.get_best_config(metric="score"))

# Get a dataframe for analyzing trial results.
df = analysis.dataframe()
# __quick_start_end__
