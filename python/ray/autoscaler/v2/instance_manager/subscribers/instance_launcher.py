import logging
import math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Optional

from ray.autoscaler._private.constants import AUTOSCALER_MAX_LAUNCH_PER_TYPE
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceUtil
from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSubscriber,
)
from ray.autoscaler.v2.instance_manager.node_provider import ICloudInstanceProvider
from ray.autoscaler.v2.schema import InvalidInstanceStatusError, NodeType
from ray.core.generated.instance_manager_pb2 import Instance, InstanceUpdateEvent

logger = logging.getLogger(__name__)


class InstanceLauncher(InstanceUpdatedSubscriber):
    """InstanceLauncher is responsible for requesting new instances.

    This subscribes to new QUEUED instances and launch them by updating
    them to REQUESTED status.
    """

    def __init__(
        self,
        instance_storage: InstanceStorage,
        node_provider: ICloudInstanceProvider,
        upscaling_speed: float,
        max_launch_per_type: int = AUTOSCALER_MAX_LAUNCH_PER_TYPE,
        launch_callback: Optional[Callable] = None,
    ) -> None:
        self._instance_storage = instance_storage
        self._node_provider = node_provider
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._upscaling_speed = upscaling_speed
        self._max_launch_per_type = max_launch_per_type
        self._launch_callback = launch_callback or self._default_launch_callback
        self._request_prefix = "instance_launcher"

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        has_new_queued = any(
            [event.new_instance_status == Instance.QUEUED for event in events]
        )

        if has_new_queued:
            fut = self._executor.submit(self._may_launch_new_instances)
            fut.add_done_callback(self._launch_callback)

    @staticmethod
    def _default_launch_callback(fut):
        """
        Default callback for launching instances.

        One could modify this for testing by passing in a different callback
        when initializing the instance launcher.

        Args:
            fut: the future of the launch request.
        """
        try:
            ret = fut.result()
        except Exception as e:
            logger.exception(f"Failed to request instances: {str(e)}")
        else:
            logger.info(f"Successfully request instances to launch: {dict(ret)}")

    def _may_launch_new_instances(self) -> Optional[Dict[str, Dict[str, int]]]:
        """
        Launches new instances if there are new instances in QUEUED status.

        In order to avoid launching too many instances at the same time, we
        limit the number of REQUESTED per node type as follows:
        - Limit the total number of REQUESTED instances to be
            max(self._max_launch_per_type, self._upscaling_speed * num_requested_or_allocated) # noqa

        where,
            - upscaling_speed is a user config from the autoscaling config.
            - requested_or_allocated[node_type] is the number of nodes of
            the same type where are requested for launch or already allocated.

        Returns:
            A dict of request_id to the number of instances to launch.

        """

        # These are all the instances that are either running ray or could
        # eventually be running ray, which constitutes the total number of
        # instances that could be running ray eventually.
        target_instances, version = self._instance_storage.get_instances(
            status_filter=InstanceUtil.ray_running_reachable_statuses()
        )

        # Use the newest version as the launch request id.
        launch_request_id = f"{self._request_prefix}:{str(version)}"

        # Instances that are in QUEUED status.
        queued_instances = []
        # Instances that are already requested for launch, or have
        # already been allocated .
        requested_or_allocated_instances = []

        for instance in target_instances.values():
            if instance.status == Instance.QUEUED:
                queued_instances.append(instance)
            else:
                requested_or_allocated_instances.append(instance)

        if not queued_instances:
            # This could happen if instances are changed to non-queued status
            # between the time we get instance update and the time we try to
            # to list all queued instances.
            logger.debug("No queued instances to launch.")
            return None

        def _group_by_type(instances):
            instances_by_type = defaultdict(list)
            for instance in instances:
                instances_by_type[instance.instance_type].append(instance)
            return instances_by_type

        queued_instances_by_type = _group_by_type(queued_instances)
        requested_or_allocated_instances_by_type = _group_by_type(
            requested_or_allocated_instances
        )

        to_launch = self._get_to_launch(
            queued_instances_by_type, requested_or_allocated_instances_by_type
        )

        # Update the instances to REQUESTED status.
        instances = []
        request_id_to_launch_shape: Dict[str, Dict[str, int]] = defaultdict(dict)

        for ins_type, instances_to_request_launch in to_launch.items():
            for ins in instances_to_request_launch:
                request_id = ins.launch_request_id or launch_request_id
                try:
                    InstanceUtil.set_status(ins, Instance.REQUESTED)
                except InvalidInstanceStatusError as e:
                    # We don't want to fail the entire launch request if we
                    # fail to set the status of one instance.
                    logger.warning(
                        f"Failed to set instance {ins.instance_id} to "
                        f"REQUESTED status: {e}"
                    )
                    continue

                ins.launch_request_id = request_id
                instances.append(ins)
                if ins_type not in request_id_to_launch_shape[request_id]:
                    request_id_to_launch_shape[request_id][ins_type] = 0
                request_id_to_launch_shape[request_id][ins_type] += 1

        # Update the instance storage.
        result = self._instance_storage.batch_upsert_instances(
            instances, expected_storage_version=version
        )
        if not result.success:
            # Unable to update the instances now - try again later.
            return None

        version = result.version
        # Update the cloud node providers.
        for request_id, to_launch in request_id_to_launch_shape.items():
            self._node_provider.launch(
                shape=to_launch,
                request_id=request_id,
            )

        return request_id_to_launch_shape

    def _get_to_launch(
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
        all_to_launch = {}
        for instance_type, instances in queued_instances_by_type.items():
            requested_or_allocated = requested_or_allocated_instances_by_type.get(
                instance_type, []
            )
            to_launch = self._get_to_launch_for_type(
                instances, len(requested_or_allocated)
            )
            all_to_launch[instance_type] = to_launch

            logger.debug(
                f"Launching {len(to_launch)} instances of {instance_type} with "
                f"{len(requested_or_allocated)} already requested for launch "
                "or allocated."
            )

        return all_to_launch

    def _get_to_launch_for_type(
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
        max_to_launch = min(
            len(queued),  # Cap num to launch to number of queued.
            min(
                self._max_launch_per_type,  # Cap num to launch to max per type.
                max(math.ceil(self._upscaling_speed * num_requested_or_allocated), 1),
            ),
        )
        assert max_to_launch > 0 and max_to_launch <= len(queued), (
            f"max_to_launch={max_to_launch} must be between 0 and "
            f"{len(queued)} (number of queued instances)"
        )

        # Sort the instances by the time they were queued.
        def _sort_by_earliest_queued(instance: Instance) -> List[int]:
            queue_times = InstanceUtil.get_status_times_ns(instance, Instance.QUEUED)
            return sorted(queue_times)

        return sorted(queued, key=_sort_by_earliest_queued)[:max_to_launch]
