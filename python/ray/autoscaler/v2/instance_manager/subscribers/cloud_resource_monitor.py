import logging
import time
from typing import Dict, List

from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.autoscaler.v2.instance_manager.instance_manager import (
    InstanceUpdatedSubscriber,
)
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.schema import NodeType
from ray.core.generated.instance_manager_pb2 import Instance, InstanceUpdateEvent

logger = logging.getLogger(__name__)


class CloudResourceAvailability:
    """CloudResourceAvailability indicates the availability of a type of
    cloud resource.

    During scaling up, if resource of a node type is requested but fail to
    allocate, that type is considered unavailable at that timestamp.
    This class records the last timestamp at which a node type is unavailable,
    allowing the autoscaler to skip such node types when making future scaling
    decisions.
    """

    # The node type of cloud resource.
    _node_type: NodeType
    # Timestamp of the last failed resource allocation.
    _last_unavailable_timestamp: int

    def __init__(self, node_type: NodeType, last_unavailability_timestamp: int):
        self._node_type = node_type
        self._last_unavailable_timestamp = last_unavailability_timestamp

    def get_last_unavailability_timestamp(self) -> int:
        return self._last_unavailable_timestamp

    def set_last_unavailability_timestamp(self, timestamp: int):
        self._last_unavailable_timestamp = timestamp


class CloudResourceMonitor(InstanceUpdatedSubscriber):
    """CloudResourceMonitor records the availability of all node types.

    In the Spot scenario, the resources in the cluster change dynamically.
    When scaling up, it is necessary to know which node types are most
    likely to have resources, in order to decide which type of node to request.
    """

    def __init__(
        self,
        instance_storage: InstanceStorage,
    ) -> None:
        self._instance_storage = instance_storage
        self._resource_availabilities: Dict[NodeType, CloudResourceAvailability] = {}

    def allocation_failed(self, failed_event: InstanceUpdateEvent):
        instances, _ = self._instance_storage.get_instances(
            instance_ids=[failed_event.instance_id]
        )
        for instance in instances.values():
            last_status = InstanceUtil.get_last_status_transition(instance)
            if last_status:
                last_unavailability_timestamp=(last_status.timestamp_ns) / 1000
            else:
                last_unavailability_timestamp = time.time()
            self._resource_availabilities[
                instance.instance_type
            ] = CloudResourceAvailability(
                node_type=instance.instance_type,
                last_unavailability_timestamp=last_unavailability_timestamp,
            )
            logger.debug(
                f"Cloud Resource Type {instance.instance_type} is "
                f"unavailable at timestamp={last_unavailability_timestamp}. "
                f"We will lower its priority in feature schedules."
            )

    def allocation_succeeded(self, succeeded_event: InstanceUpdateEvent):
        if succeeded_event.instance_type in self._resource_availabilities:
            self._resource_availabilities[
                succeeded_event.instance_type
            ].set_last_unavailability_timestamp(0)
        logger.debug(
            f"Cloud Resource Type {succeeded_event.instance_type} is "
            f"available. We will prioritize scheduling this type "
            f"in feature schedules."
        )

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        for event in events:
            if event.new_instance_status == Instance.ALLOCATION_FAILED:
                self.allocation_failed(event)
            elif event.new_instance_status == Instance.ALLOCATED:
                self.allocation_succeeded(event)

    def get_resource_availabilities(self) -> Dict[NodeType, float]:
        """Calculate the availability scores of node types.
        Higher values indicate a higher likelihood of resource allocation.
        """
        resource_availability_scores: Dict[NodeType, float] = {}
        if self._resource_availabilities:
            max_ts = max(
                [
                    r.get_last_unavailability_timestamp()
                    for r in self._resource_availabilities.values()
                ]
            )
            for node_type in self._resource_availabilities:
                resource_availability_scores[node_type] = (
                    (
                        1
                        - self._resource_availabilities[
                            node_type
                        ].get_last_unavailability_timestamp()
                        / max_ts
                    )
                    if max_ts > 0
                    else 1
                )
        return resource_availability_scores
