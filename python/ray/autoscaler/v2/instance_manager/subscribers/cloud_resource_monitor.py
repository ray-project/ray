import logging
import time
from typing import Dict, List

from ray.autoscaler._private.constants import RAY_AUTOSCALER_AVAILABILITY_RECOVERY_S
from ray.autoscaler.v2.instance_manager.instance_manager import (
    InstanceUpdatedSubscriber,
)
from ray.autoscaler.v2.schema import NodeType
from ray.core.generated.instance_manager_pb2 import Instance, InstanceUpdateEvent

logger = logging.getLogger(__name__)


class CloudResourceMonitor(InstanceUpdatedSubscriber):
    """CloudResourceMonitor records the availability of all node types.

    In the Spot scenario, the resources in the cluster change dynamically.
    When scaling up, it is necessary to know which node types are most
    likely to have resources, in order to decide which type of node to request.

    During scaling up, if resource of a node type is requested but fail to
    allocate, that type is considered unavailable at that timestamp.This class
    records the last timestamp at which a node type is unavailable,allowing the
    autoscaler to skip such node types when making future scaling decisions.
    """

    def __init__(
        self,
    ) -> None:
        self._last_unavailable_timestamp: Dict[NodeType, float] = {}

    def allocation_timeout(self, failed_event: InstanceUpdateEvent):
        unavailable_timestamp = time.time()
        self._last_unavailable_timestamp[
            failed_event.instance_type
        ] = unavailable_timestamp
        logger.info(
            f"Cloud Resource Type {failed_event.instance_type} is "
            f"unavailable at timestamp={unavailable_timestamp}. "
            f"We will lower its priority in feature schedules."
        )

    def allocation_succeeded(self, succeeded_event: InstanceUpdateEvent):
        if succeeded_event.instance_type in self._last_unavailable_timestamp:
            self._last_unavailable_timestamp.pop(succeeded_event.instance_type)
            logger.info(
                f"Cloud Resource Type {succeeded_event.instance_type} is "
                f"available at timestamp={time.time()}. We will higher its priority in "
                f"feature schedules."
            )

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        for event in events:
            if event.new_instance_status == Instance.ALLOCATION_TIMEOUT:
                self.allocation_timeout(event)
            elif (
                event.new_instance_status == Instance.RAY_RUNNING
                and event.instance_type
            ):
                self.allocation_succeeded(event)

    def get_resource_availabilities(self) -> Dict[NodeType, float]:
        """Calculate the availability scores of node types.
        Higher values indicate a higher likelihood of resource allocation.
        """
        resource_availability_scores: Dict[NodeType, float] = {}
        if self._last_unavailable_timestamp:
            max_ts = max(self._last_unavailable_timestamp.values())
            for node_type in self._last_unavailable_timestamp:
                resource_availability_scores[node_type] = (
                    1 - self._last_unavailable_timestamp[node_type] / max_ts
                )
        return resource_availability_scores

    def get_recoverable_resource_availabilities(self) -> Dict[NodeType, float]:
        """Calculate a continuous recovery score from 0.0 to 1.0.

        score = 0.0 if (current_time - last_unavailable_timestamp) < safety_floor
        else min(1.0, (current_time - last_unavailable_timestamp) /
        RAY_AUTOSCALER_AVAILABILITY_RECOVERY_S)
        """
        assert (
            RAY_AUTOSCALER_AVAILABILITY_RECOVERY_S > 0
        ), "RAY_AUTOSCALER_AVAILABILITY_RECOVERY_S must be positive"
        recovery_scores: Dict[NodeType, float] = {}
        current_time = time.time()

        # Safety floor is 10s or 10% of recovery window.
        # This ensures that we don't immediately retry a failed node type
        # and be stuck in a retry loop.
        safety_floor = min(10, RAY_AUTOSCALER_AVAILABILITY_RECOVERY_S * 0.1)

        for node_type, last_ts in self._last_unavailable_timestamp.items():
            diff = current_time - last_ts
            if diff < safety_floor:
                recovery_scores[node_type] = 0.0
            else:
                recovery_scores[node_type] = min(
                    1.0, diff / RAY_AUTOSCALER_AVAILABILITY_RECOVERY_S
                )
        return recovery_scores
