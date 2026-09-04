import enum
import logging
import os
from typing import TYPE_CHECKING

from .base_autoscaling_coordinator import (
    AutoscalingCoordinator,
    ResourceDict,
    ResourceRequestPriority,
)
from .base_cluster_autoscaler import ClusterAutoscaler
from .default_autoscaling_coordinator import (
    DefaultAutoscalingCoordinator,
    get_or_create_autoscaling_coordinator,
)
from .default_cluster_autoscaler_v2 import DefaultClusterAutoscalerV2
from .placement_group_cluster_autoscaler import PlacementGroupClusterAutoscaler
from .rate_based_cluster_autoscaler import RateBasedClusterAutoscaler
from .supports_cluster_autoscaling import (
    ClusterAutoscalingMetrics,
    SupportsClusterAutoscaling,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data.context import DataContext

logger = logging.getLogger(__name__)

CLUSTER_AUTOSCALER_ENV_KEY = "RAY_DATA_CLUSTER_AUTOSCALER"
DEFAULT_CLUSTER_AUTOSCALER_VERSION = "V2"


class ClusterAutoscalerVersion(str, enum.Enum):
    V2 = "V2"
    RATE_BASED = "RATE_BASED"


def create_cluster_autoscaler(
    topology: "Topology",
    resource_manager: "ResourceManager",
    data_context: "DataContext",
    *,
    execution_id: str,
) -> ClusterAutoscaler:
    resource_limits = data_context.execution_options.resource_limits
    label_selector = data_context.execution_options.label_selector
    cluster_autoscaler_version = os.environ.get(
        CLUSTER_AUTOSCALER_ENV_KEY, DEFAULT_CLUSTER_AUTOSCALER_VERSION
    )
    logger.debug(f"Using cluster autoscaler version: {cluster_autoscaler_version!r}")

    # When users specify a PlacementGroupSchedulingStrategy, the PG bundles
    # already define the exact resources needed. The regular autoscaler would
    # scale up nodes that don't actually help, so we use a simpler implementation
    # that just requests the PG bundles directly.
    if isinstance(
        data_context.scheduling_strategy, PlacementGroupSchedulingStrategy
    ) and isinstance(
        data_context.scheduling_strategy_large_args,
        PlacementGroupSchedulingStrategy,
    ):
        return PlacementGroupClusterAutoscaler(
            execution_id=execution_id,
            scheduling_strategy=data_context.scheduling_strategy,
            scheduling_strategy_large_args=data_context.scheduling_strategy_large_args,
        )

    elif cluster_autoscaler_version == ClusterAutoscalerVersion.RATE_BASED:
        return RateBasedClusterAutoscaler.create(
            topology,  # pyrefly: ignore[bad-argument-type]
            data_context.execution_options,
            resource_manager,
            execution_id=execution_id,
        )

    elif cluster_autoscaler_version == ClusterAutoscalerVersion.V2:
        return DefaultClusterAutoscalerV2(
            resource_manager,
            execution_id=execution_id,
            resource_limits=resource_limits,
            label_selector=label_selector,
        )

    else:
        valid_values = [version.value for version in ClusterAutoscalerVersion]
        raise ValueError(
            f"Cluster autoscaler version of {cluster_autoscaler_version} isn't a valid "
            f"option. Valid options are: {valid_values}."
        )


__all__ = [
    "ClusterAutoscaler",
    "RateBasedClusterAutoscaler",
    "SupportsClusterAutoscaling",
    "ClusterAutoscalingMetrics",
    # Objects related to the `AutoscalingCoordinator`.
    "AutoscalingCoordinator",
    "DefaultAutoscalingCoordinator",
    "get_or_create_autoscaling_coordinator",
    "ResourceDict",
    "ResourceRequestPriority",
]
