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
from ray import train, tune


def objective(config):  # <1>
    score = config["a"] ** 2 + config["b"]
    return {"score": score}


search_space = {  # <2>
    "a": tune.grid_search([0.001, 0.01, 0.1, 1.0]),
    "b": tune.choice([1, 2, 3]),
}

tuner = tune.Tuner(objective, param_space=search_space)  # <3>

results = tuner.fit()
print(results.get_best_result(metric="score", mode="min").config)
# __quick_start_end__

# __ml_quick_start_begin__
def objective(step, alpha, beta):
    return (0.1 + alpha * step / 100) ** (-1) + beta * 0.1


def training_function(config):
    # Hyperparameters
    alpha, beta = config["alpha"], config["beta"]
    for step in range(10):
        # Iterative training function - can be any arbitrary training procedure.
        intermediate_score = objective(step, alpha, beta)
        # Feed the score back back to Tune.
        train.report({"mean_loss": intermediate_score})


tuner = tune.Tuner(
    training_function,
    param_space={
        "alpha": tune.grid_search([0.001, 0.01, 0.1]),
        "beta": tune.choice([1, 2, 3]),
    },
)
results = tuner.fit()

print("Best config: ", results.get_best_result(metric="mean_loss", mode="min").config)

# Get a dataframe for analyzing trial results.
df = results.get_dataframe()
# __ml_quick_start_end__
