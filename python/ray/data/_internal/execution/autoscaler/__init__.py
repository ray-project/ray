from .autoscaler import Autoscaler
from .autoscaling_actor_pool import AutoscalingActorPool
from .default_autoscaler import DefaultAutoscaler


def create_autoscaler(topology, resource_manager, execution_id):
    return DefaultAutoscaler(topology, resource_manager, execution_id)


__all__ = [
    "Autoscaler",
    "DefaultAutoscaler",
    "create_autoscaler",
    "AutoscalingActorPool",
]
