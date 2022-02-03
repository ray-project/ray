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


def objective(step, alpha, beta):
    return (0.1 + alpha * step / 100) ** (-1) + beta * 0.1


def training_function(config):
    # Hyperparameters
    alpha, beta = config["alpha"], config["beta"]
    for step in range(10):
        # Iterative training function - can be any arbitrary training procedure.
        intermediate_score = objective(step, alpha, beta)
        # Feed the score back back to Tune.
        tune.report(mean_loss=intermediate_score)


analysis = tune.run(
    training_function,
    config={
        "alpha": tune.grid_search([0.001, 0.01, 0.1]),
        "beta": tune.choice([1, 2, 3]),
    },
)

print("Best config: ", analysis.get_best_config(metric="mean_loss", mode="min"))

# Get a dataframe for analyzing trial results.
df = analysis.results_df
# __quick_start_end__
