from typing import TYPE_CHECKING

from .autoscaler import Autoscaler
from .autoscaling_actor_pool import AutoscalingActorPool
from .default_autoscaler import DefaultAutoscaler

if TYPE_CHECKING:
    from ..resource_manager import ResourceManager
    from ..streaming_executor_state import Topology
    from ray.data.context import AutoscalingConfig


def create_autoscaler(
    topology: "Topology",
    resource_manager: "ResourceManager",
    config: "AutoscalingConfig",
    *,
    execution_id: str
) -> Autoscaler:
    return DefaultAutoscaler(
        topology,
        resource_manager,
        config=config,
        execution_id=execution_id,
    )


__all__ = [
    "Autoscaler",
    "DefaultAutoscaler",
    "create_autoscaler",
    "AutoscalingActorPool",
]
