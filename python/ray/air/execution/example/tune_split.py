import ray
from ray import tune
from ray.air.execution.impl.tune.progress_loop import tune_split
from ray.tune.search import BasicVariantGenerator


ray.init()


def train_fn(config):
    return {"metric": config["A"] - config["B"]}


tune_split(
    trainable=train_fn,
    num_samples=4,
    param_space={
        "A": tune.randint(0, 100),
        "B": tune.grid_search([1, 2, 3, 4]),
    },
    split_by="B",
    search_alg=BasicVariantGenerator(max_concurrent=4),
    resources_per_split={"CPU": 8},
)
