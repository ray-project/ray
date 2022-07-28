from ray import tune
from ray.air import session


def objective(step, alpha, beta):
    return (0.1 + alpha * step / 100) ** (-1) + beta * 0.1


def training_function(config):
    # Hyperparameters
    alpha, beta = config["alpha"], config["beta"]
    for step in range(10):
        # Iterative training function - can be any arbitrary training procedure.
        intermediate_score = objective(step, alpha, beta)
        # Feed the score back back to Tune.
        session.report({"mean_loss": intermediate_score})


tuner = tune.Tuner(
    training_function,
    param_space={
        "alpha": tune.grid_search([0.001, 0.01, 0.1]),
        "beta": tune.choice([1, 2, 3]),
    },
)
results = tuner.fit()

best_result = results.get_best_result(metric="mean_loss", mode="min")
print("Best result: ", best_result.metrics)

# Get a dataframe for analyzing trial results.
df = results.get_dataframe()
