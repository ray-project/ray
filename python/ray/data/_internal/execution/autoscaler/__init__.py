from .autoscaler import Autoscaler
from .autoscaling_actor_pool import AutoscalingActorPool
from .default_autoscaler import DefaultAutoscaler


def create_autoscaler(topology, resource_manager, execution_id):
    from ray._private.ray_constants import env_bool

    if env_bool("RAY_DATA_ENABLE_ANYSCALE_AUTOSCALER", True):
        from ray.anyscale.data.autoscaler.anyscale_autoscaler import AnyscaleAutoscaler

        return AnyscaleAutoscaler(topology, resource_manager, execution_id)
    else:
        return DefaultAutoscaler(topology, resource_manager, execution_id)


__all__ = [
    "Autoscaler",
    "DefaultAutoscaler",
    "create_autoscaler",
    "AutoscalingActorPool",
]
