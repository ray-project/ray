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

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data.context import DataContext

logger = logging.getLogger(__name__)

CLUSTER_AUTOSCALER_ENV_KEY = "RAY_DATA_CLUSTER_AUTOSCALER"
DEFAULT_CLUSTER_AUTOSCALER_VERSION = "RAYTURBO"


class ClusterAutoscalerVersion(str, enum.Enum):
    RAYTURBO = "RAYTURBO"
    V2 = "V2"


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

    if cluster_autoscaler_version == ClusterAutoscalerVersion.RAYTURBO:
        from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

        # When users specify a PlacementGroupSchedulingStrategy, the PG bundles
        # already define the exact resources needed. The regular autoscaler would
        # scale up nodes that don't actually help, so we use a simpler implementation
        # that just requests the PG bundles directly.
        #
        # NOTE: This isn't a common case. I'm adding it to appease EarthDaily, a
        # prospective Anyscale user who reported the issue here:
        # https://github.com/ray-project/ray/issues/62699. Long-term, we should
        # eliminate the Ray Data cluster autoscaler and/or these two config options.
        if isinstance(
            data_context.scheduling_strategy, PlacementGroupSchedulingStrategy
        ) and isinstance(
            data_context.scheduling_strategy_large_args,
            PlacementGroupSchedulingStrategy,
        ):
            from ray.anyscale.data._internal.cluster_autoscaler import (
                PlacementGroupClusterAutoscaler,
            )

            return PlacementGroupClusterAutoscaler(
                execution_id=execution_id,
                scheduling_strategy=data_context.scheduling_strategy,
                scheduling_strategy_large_args=data_context.scheduling_strategy_large_args,
            )

        from ray.anyscale.data._internal.cluster_autoscaler import (
            RateBasedClusterAutoscaler,
        )

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
    # Objects related to the `AutoscalingCoordinator`.
    "AutoscalingCoordinator",
    "DefaultAutoscalingCoordinator",
    "get_or_create_autoscaling_coordinator",
    "ResourceDict",
    "ResourceRequestPriority",
]
