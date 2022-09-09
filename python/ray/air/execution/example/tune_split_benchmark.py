import time
import timeit

import ray
from ray import tune
from ray.air.execution.impl.tune.progress_loop import tune_split, tune_run
from ray.tune.search import BasicVariantGenerator


ray.init()


def train_fn(config):
    time.sleep(0.5)
    return {"metric": config["A"] - config["B"]}


time_no_split = timeit.timeit(
    lambda: tune_run(
        trainable=train_fn,
        num_samples=16,
        param_space={
            "A": tune.randint(0, 100),
            "B": tune.grid_search([1, 2, 3, 4]),
        },
        search_alg=BasicVariantGenerator(),
    ),
    number=1,
)

print(f"No split: {time_no_split:.2f} seconds")

time_split = timeit.timeit(
    lambda: tune_split(
        trainable=train_fn,
        num_samples=16,
        param_space={
            "A": tune.randint(0, 100),
            "B": tune.grid_search([1, 2, 3, 4]),
        },
        split_by="B",
        search_alg=BasicVariantGenerator(),
        resources_per_split={"CPU": 4},
    ),
    number=1,
)

print(f"No split: {time_no_split:.2f} seconds")
print(f"Split: {time_split:.2f} seconds")
