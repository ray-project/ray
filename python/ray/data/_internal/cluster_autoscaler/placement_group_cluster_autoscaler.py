import logging
import time
from typing import Dict, List, Optional

from ray.data._internal.cluster_autoscaler import (
    AutoscalingCoordinator,
    DefaultAutoscalingCoordinator,
)
from ray.data._internal.cluster_autoscaler.base_cluster_autoscaler import (
    ClusterAutoscaler,
)
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

logger = logging.getLogger(__name__)


class PlacementGroupClusterAutoscaler(ClusterAutoscaler):
    """Cluster autoscaler for placement-group-based scheduling strategies.

    When users configure a PlacementGroupSchedulingStrategy, the exact resources
    they need are already defined by the placement group's bundles. This autoscaler
    simply requests those bundles instead of using a more sophisticated algorithm.
    """

    # Min number of seconds between two autoscaling requests.
    MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS_S = 10

    # The time in seconds after which an autoscaling request will expire.
    AUTOSCALING_REQUEST_EXPIRE_TIME_S = 180

    def __init__(
        self,
        execution_id: str,
        scheduling_strategy: PlacementGroupSchedulingStrategy,
        scheduling_strategy_large_args: PlacementGroupSchedulingStrategy,
        *,
        min_gap_between_autoscaling_requests_s: int = MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS_S,
        autoscaling_request_expire_time_s: int = AUTOSCALING_REQUEST_EXPIRE_TIME_S,
        autoscaling_coordinator: Optional[AutoscalingCoordinator] = None,
    ):
        assert isinstance(
            scheduling_strategy, PlacementGroupSchedulingStrategy
        ), f"Expected PlacementGroupSchedulingStrategy, got {type(scheduling_strategy)}"
        assert isinstance(
            scheduling_strategy_large_args, PlacementGroupSchedulingStrategy
        ), (
            f"Expected PlacementGroupSchedulingStrategy, "
            f"got {type(scheduling_strategy_large_args)}"
        )

        pg = scheduling_strategy.placement_group
        pg_large = scheduling_strategy_large_args.placement_group

        if autoscaling_coordinator is None:
            autoscaling_coordinator = DefaultAutoscalingCoordinator(
                requester_id=f"data-{execution_id}"
            )

        self._execution_id = execution_id
        self._requester_id = f"data-{execution_id}"
        self._min_gap_between_autoscaling_requests_s = (
            min_gap_between_autoscaling_requests_s
        )
        self._autoscaling_request_expire_time_s = autoscaling_request_expire_time_s
        self._autoscaling_coordinator = autoscaling_coordinator

        self._last_request_time = 0.0

        if pg.id == pg_large.id:
            self._bundle_specs = _get_bundle_specs(pg)
        else:
            self._bundle_specs = _get_bundle_specs(pg) + _get_bundle_specs(pg_large)

        logger.debug(
            f"PlacementGroupClusterAutoscaler initialized with "
            f"{len(self._bundle_specs)} bundles: {self._bundle_specs}"
        )

        # Send an initial request to register ourselves.
        self._send_resource_request(self._bundle_specs)

    def try_trigger_scaling(self):
        now = time.monotonic()
        if now - self._last_request_time < self._min_gap_between_autoscaling_requests_s:
            return

        self._send_resource_request(self._bundle_specs)

    def on_executor_shutdown(self):
        try:
            self._autoscaling_coordinator.cancel_request()
        except Exception:
            logger.warning(
                f"Failed to cancel resource request for {self._requester_id}.",
                exc_info=True,
            )

    def get_total_resources(self) -> ExecutionResources:
        # Return the PG bundle resources directly rather than querying the coordinator's
        # allocations. With placement groups, the resources are scheduled upfront, so
        # it doesn't make sense to query the coordinator's allocations.
        total = ExecutionResources.zero()
        for res in self._bundle_specs:
            total = total.add(ExecutionResources.from_resource_dict(res))
        return total

    def _send_resource_request(self, resource_request: List[Dict[str, float]]):
        logger.debug(f"Sending resource request with {len(resource_request)} bundles")
        self._autoscaling_coordinator.request_resources(
            resources=resource_request.copy(),
            expire_after_s=self._autoscaling_request_expire_time_s,
            request_remaining=False,
        )
        self._last_request_time = time.monotonic()


def _get_bundle_specs(pg: PlacementGroup) -> List[Dict[str, float]]:
    """Extract bundle specs from a placement group, filtering out zero values."""
    return [{k: v for k, v in bundle.items() if v > 0} for bundle in pg.bundle_specs]
