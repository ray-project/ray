import ray

from ray import tune

ray.init(address="auto")


def objective(config):
    score = config["a"] ** 2 + config["b"]
    return {"score": score}


search_space = {
    "a": tune.grid_search([0.001, 0.01, 0.1, 1.0]),
    "b": tune.choice([1, 2, 3]),
}

tuner = tune.Tuner(objective, param_space=search_space)

print("Starting Ray Tune job")
results = tuner.fit()

print("Best config: ")
print(results.get_best_result(metric="score", mode="min").config)
