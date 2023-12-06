import ray
from ray.util.scheduling_strategies import (
    In,
    NotIn,
    Exists,
    DoesNotExist,
    NodeLabelSchedulingStrategy,
)
ray.init()


@ray.remote
class MyActor:
    def __init__(self):
        self.value = 0

    def value(self):
        return self.value

    def get_node_id(self):
        return ray.get_runtime_context().get_node_id()


actor = MyActor.options(
    scheduling_strategy=NodeLabelSchedulingStrategy(
        hard={"gpu_type": In("A100")}
    )
    ).remote()


node_id = ray.get(actor.get_node_id.remote())
print(node_id)