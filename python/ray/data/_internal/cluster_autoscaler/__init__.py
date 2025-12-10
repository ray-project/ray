import os
from enum import Enum
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


CLUSTER_AUTOSCALER_ENV_KEY = "RAY_DATA_CLUSTER_AUTOSCALER"
CLUSTER_AUTOSCALER_ENV_DEFAULT_VALUE = "V1"


class ClusterAutoscalerVersion(Enum):
    V2 = "V2"
    V1 = "V1"


def create_cluster_autoscaler(
    topology: "Topology", resource_manager: "ResourceManager", *, execution_id: str
) -> ClusterAutoscaler:
    selected_autoscaler = _get_cluster_autoscaler_version()

    if selected_autoscaler == ClusterAutoscalerVersion.V2:
        return DefaultClusterAutoscalerV2(
            topology,
            resource_manager,
            execution_id=execution_id,
        )

    elif selected_autoscaler == ClusterAutoscalerVersion.V1:
        return DefaultClusterAutoscaler(
            topology,
            resource_manager,
            execution_id=execution_id,
        )

    assert False, "Invalid cluster autoscaler option"


def _get_cluster_autoscaler_version() -> ClusterAutoscalerVersion:
    cluster_autoscaler_env_value = os.getenv(
        CLUSTER_AUTOSCALER_ENV_KEY, CLUSTER_AUTOSCALER_ENV_DEFAULT_VALUE
    )
    try:
        return ClusterAutoscalerVersion(cluster_autoscaler_env_value)
    except ValueError:
        valid_values = [version.value for version in ClusterAutoscalerVersion]
        raise ValueError(
            f"{cluster_autoscaler_env_value} isn't a valid option for "
            f"{CLUSTER_AUTOSCALER_ENV_KEY}. Valid options are: {valid_values}."
        )


__all__ = [
    "ClusterAutoscaler",
    # Objects related to the `AutoscalingCoordinator`.
    "AutoscalingCoordinator",
    "DefaultAutoscalingCoordinator",
    "ResourceDict",
    "ResourceRequestPriority",
]
