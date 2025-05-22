from .autoscaler import Autoscaler
from .autoscaling_actor_pool import AutoscalingActorPool
from .default_autoscaler import DefaultAutoscaler
from ray.data.context import DataContext


def create_autoscaler(topology, resource_manager, execution_id):
    ctx = DataContext.get_current()

    if ctx.custom_autoscaler is None:
        return DefaultAutoscaler(topology, resource_manager, execution_id)
    else:
        return ctx.custom_autoscaler(topology, resource_manager, execution_id)


__all__ = [
    "Autoscaler",
    "DefaultAutoscaler",
    "create_autoscaler",
    "AutoscalingActorPool",
]
