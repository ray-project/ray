import logging
import math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List
from ray.autoscaler._private.constants import AUTOSCALER_MAX_LAUNCH_PER_TYPE

from ray.autoscaler.v2.instance_manager.instance_manager import InstanceUtil
from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSubscriber,
)
from ray.autoscaler.v2.instance_manager.node_provider import (
    ICloudNodeProvider,
    NodeProvider,
)
from ray.autoscaler.v2.schema import InvalidInstanceStatusError, NodeType
from ray.core.generated.instance_manager_pb2 import Instance, InstanceUpdateEvent

logger = logging.getLogger(__name__)


class InstanceLauncher(InstanceUpdatedSubscriber):
    """InstanceLauncher is responsible for provisioning new instances.

    This subscribes to new QUEUED instances and launch them by updating
    them to REQUESTED status.
    """

    def __init__(
        self,
        instance_storage: InstanceStorage,
        node_provider: ICloudNodeProvider,
        upscaling_speed: float,
        max_launch_per_type: int = AUTOSCALER_MAX_LAUNCH_PER_TYPE,
    ) -> None:
        self._instance_storage = instance_storage
        self._node_provider = node_provider
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._upscaling_speed = upscaling_speed
        self._max_launch_per_type = max_launch_per_type

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        has_new_queued = any(
            [event.new_instance_status == Instance.QUEUED for event in events]
        )
        if has_new_queued:
            self._executor.submit(self._may_launch_new_instances)

    def _may_launch_new_instances(self):
        """
        Launches new instances if there are new instances in QUEUED status.

        In order to avoid launching too many instances at the same time, we
        limit the number of REQUESTED per node type as follows:
        - Limit the total number of REQUESTED instances to be
            max(5, upscaling_speed * max(requested_or_allocated[node_type]), 1)

        where,
            - upscaling_speed is a user config from the autoscaling config.
            - requested_or_allocated[node_type] is the number of nodes of
            the same type where are requested or already allocated.

        """
        # These are all the instances that are either running ray or could
        # eventually be running ray, which constitutes the total number of
        # instances that could be running ray eventually.
        target_instances, version = self._instance_storage.get_instances(
            status_filter=InstanceUtil.ray_running_reachable_statuses()
        )

        queued_instances = []
        # Instances that are already requested, or have already been allocated .
        requested_or_allocated_instances = []

        for instance in target_instances.values():
            if instance.status == Instance.QUEUED:
                queued_instances.append(instance)
            else:
                requested_or_allocated_instances.append(instance)

        if not queued_instances:
            logger.debug("No queued instances to launch")
            return

        def group_by_type(instances):
            instances_by_type = defaultdict(list)
            for instance in instances:
                instances_by_type[instance.instance_type].append(instance)
            return instances_by_type

        queued_instances_by_type = group_by_type(queued_instances)
        requested_or_allocated_instances_by_type = group_by_type(
            requested_or_allocated_instances
        )

        to_request = self._get_to_request(
            queued_instances_by_type, requested_or_allocated_instances_by_type
        )

        # Update the instances to REQUESTED status.
        instances = []
        target_node_count_by_type = defaultdict(int)
        for (
            ins_type,
            instances_to_request,
        ) in requested_or_allocated_instances_by_type.items():
            target_node_count_by_type[ins_type] = len(instances_to_request)

        for ins_type, instances_to_request in to_request.items():
            for ins in instances_to_request:
                try:
                    InstanceUtil.set_status(ins, Instance.REQUESTED)
                except InvalidInstanceStatusError as e:
                    logger.warning(
                        f"Failed to set instance {ins.instance_id} to "
                        f"REQUESTED status: {e}"
                    )
                    continue
                instances.append(ins)
                target_node_count_by_type[ins_type] += 1

        # Update the instance storage.
        result = self._instance_storage.batch_upsert_instances(
            instances, expected_storage_version=version
        )
        if not result.success:
            # Unable to update the instances now - try again later.
            return

        version = result.version
        # Update the cloud node providers.
        self._node_provider.update(
            id=str(version),
            target_running_nodes=target_node_count_by_type,
            to_terminate=[],
        )

    def _get_to_request(
        self,
        queued_instances_by_type: Dict[str, List[Instance]],
        requested_or_allocated_instances_by_type: Dict[NodeType, List[Instance]],
    ) -> Dict[NodeType, List[Instance]]:
        """Returns the number of instances to request for each node type.

        Args:
            queued_instances_by_type: a dict of instances to launch grouped by
                instance type.
            requested_or_allocated_instances_by_type: a dict of instances
                requested or allocated grouped by instance type.
        Returns:
            a dict of instances to request grouped by instance type.
        """
        to_request = {}
        for instance_type, instances in queued_instances_by_type.items():
            requested_or_allocated = requested_or_allocated_instances_by_type.get(
                instance_type, []
            )
            to_request[instance_type] = self._get_to_request_for_type(
                instances, len(requested_or_allocated)
            )

        return to_request

    def _get_to_request_for_type(
        self,
        queued: List[Instance],
        num_requested_or_allocated: int,
    ) -> List[Instance]:
        """Get the instances to request for launch.

        Args:
            instance_type: the instance type to launch.
            queued: the queued instances of the given type.
            num_requested_or_allocated: the number of instances of the given type
                that are already requested or allocated.

        Returns:
            a list of instances to launch.
        """

        # Limit the number of instances to launch at a time.
        max_to_launch = max(
            self._max_launch_per_type,
            max(math.ceil(self._upscaling_speed * num_requested_or_allocated), 1),
        )
        assert max_to_launch > 0 and max_to_launch <= len(queued)

        # Sort the instances by the time they were queued.
        def _sort_by_earliest_queued(instance: Instance) -> List[int]:
            queue_times = InstanceUtil.get_status_times_ns(instance, Instance.QUEUED)
            return sorted(queue_times)

        return sorted(queued, key=_sort_by_earliest_queued)[:max_to_launch]
