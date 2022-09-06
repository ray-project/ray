from ray import tune
from ray.air.execution.impl.tune.progress_loop import tune_run
from ray.air.execution.impl.tune.tune_controller import TuneController
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.tune.search import BasicVariantGenerator
from ray.tune.trainable import wrap_function


def train_fn(config):
    if config["fail"]:
        raise RuntimeError("Failing training")
    return {"metric": config["A"] - config["B"]}


tune_controller = TuneController(
    trainable_cls=wrap_function(train_fn),
    param_space={
        "A": tune.randint(0, 100),
        "B": tune.randint(20, 30),
        "fail": tune.grid_search([True, False]),
    },
    search_alg=BasicVariantGenerator(max_concurrent=4),
    resource_manager=FixedResourceManager(total_resources={"CPU": 4}),
)
tune_run(tune_controller=tune_controller)
