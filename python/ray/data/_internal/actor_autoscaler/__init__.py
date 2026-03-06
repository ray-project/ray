from typing import TYPE_CHECKING

from .actor_pool_resizing_policy import (
    ActorPoolResizingPolicy,
    DefaultResizingPolicy,
)
from .autoscaling_actor_pool import ActorPoolScalingRequest, AutoscalingActorPool
from .base_actor_autoscaler import ActorAutoscaler
from .default_actor_autoscaler import DefaultActorAutoscaler, _get_max_scale_up

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
    "ActorPoolResizingPolicy",
    "ActorPoolScalingRequest",
    "AutoscalingActorPool",
    "DefaultResizingPolicy",
    "create_actor_autoscaler",
    "_get_max_scale_up",
]
