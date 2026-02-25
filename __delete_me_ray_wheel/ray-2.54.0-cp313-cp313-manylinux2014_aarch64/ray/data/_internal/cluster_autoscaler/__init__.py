import enum
import os
from typing import TYPE_CHECKING

from .base_autoscaling_coordinator import (
    AutoscalingCoordinator,
    ResourceDict,
    ResourceRequestPriority,
)
from .base_cluster_autoscaler import ClusterAutoscaler
from .default_autoscaling_coordinator import DefaultAutoscalingCoordinator
from .default_cluster_autoscaler import DefaultClusterAutoscaler
from .default_cluster_autoscaler_v2 import DefaultClusterAutoscalerV2

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data.context import DataContext


DEFAULT_CLUSTER_AUTOSCALER_VERSION = os.environ.get("RAY_DATA_CLUSTER_AUTOSCALER", "V2")


class ClusterAutoscalerVersion(str, enum.Enum):
    V2 = "V2"
    V1 = "V1"


def create_cluster_autoscaler(
    topology: "Topology",
    resource_manager: "ResourceManager",
    data_context: "DataContext",
    *,
    execution_id: str,
) -> ClusterAutoscaler:
    resource_limits = data_context.execution_options.resource_limits

    if DEFAULT_CLUSTER_AUTOSCALER_VERSION == ClusterAutoscalerVersion.V2:
        return DefaultClusterAutoscalerV2(
            resource_manager,
            execution_id=execution_id,
            resource_limits=resource_limits,
        )

    elif DEFAULT_CLUSTER_AUTOSCALER_VERSION == ClusterAutoscalerVersion.V1:
        return DefaultClusterAutoscaler(
            topology,
            resource_limits=resource_limits,
            execution_id=execution_id,
        )

    else:
        valid_values = [version.value for version in ClusterAutoscalerVersion]
        raise ValueError(
            f"Cluster autoscaler version of {DEFAULT_CLUSTER_AUTOSCALER_VERSION} "
            f"isn't a valid option. Valid options are: {valid_values}."
        )


__all__ = [
    "ClusterAutoscaler",
    # Objects related to the `AutoscalingCoordinator`.
    "AutoscalingCoordinator",
    "DefaultAutoscalingCoordinator",
    "ResourceDict",
    "ResourceRequestPriority",
]
