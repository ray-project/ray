from typing import TYPE_CHECKING

from .autoscaling_actor_pool import ActorPoolScalingRequest, AutoscalingActorPool
from .base_actor_autoscaler import ActorAutoscaler
from .default_actor_autoscaler import DefaultActorAutoscaler

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data.context import AutoscalingConfig


def create_actor_autoscaler(
    topology: "Topology",
    resource_manager: "ResourceManager",
    config: "AutoscalingConfig",
) -> ActorAutoscaler:
    return DefaultActorAutoscaler(
        topology,
        resource_manager,
        config=config,
    )


__all__ = [
    "ActorAutoscaler",
    "ActorPoolScalingRequest",
    "AutoscalingActorPool",
    "create_actor_autoscaler",
]
