from ray import tune
from ray.air.execution.actor_manager import ActorManager
from ray.air.execution.impl.tune.tune_controller import TuneController
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.tune.trainable import wrap_function


def train_fn(config):
    return config["A"] - config["B"]


tune_controller = TuneController(
    trainable_cls=wrap_function(train_fn),
    param_space={"A": tune.randint(0, 100), "B": tune.randint(20, 30)},
)
fixed_resource_manager = FixedResourceManager(total_resources={"CPU": 4})
manager = ActorManager(
    controller=tune_controller, resource_manager=fixed_resource_manager
)
