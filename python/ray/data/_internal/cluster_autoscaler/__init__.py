from typing import TYPE_CHECKING

from .base_cluster_autoscaler import ClusterAutoscaler
from .default_cluster_autoscaler import DefaultClusterAutoscaler

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology


def create_cluster_autoscaler(
    topology: "Topology", resource_manager: "ResourceManager", *, execution_id: str
) -> ClusterAutoscaler:
    return DefaultClusterAutoscaler(
        topology, resource_manager, execution_id=execution_id
    )


__all__ = ["ClusterAutoscaler"]
