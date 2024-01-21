import logging
import math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Optional

from ray.autoscaler._private.constants import AUTOSCALER_MAX_CONCURRENT_LAUNCHES
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
        max_concurrent_launches: int = AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
        launch_callback: Optional[Callable] = None,
    ) -> None:
        """
        Args:
            instance_storage: the instance storage.
            node_provider: the node provider.
            upscaling_speed: the upscaling speed.
            max_concurrent_launches: the maximum number of concurrent launches.
            launch_callback: the callback to call when launching instances are
                done in the background.
        """
        self._instance_storage = instance_storage
        self._node_provider = node_provider
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._upscaling_speed = upscaling_speed
        self._max_concurrent_launches = max_concurrent_launches
        self._launch_callback = launch_callback or self._default_launch_callback
        self._request_prefix = "instance_launcher"

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        has_new_queued = any(
            [event.new_instance_status == Instance.QUEUED for event in events]
        )

        if has_new_queued:
            fut = self._executor.submit(self._may_launch_new_instances)
            fut.add_done_callback(self._launch_callback)

    ############################
    # Private functions
    ############################

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

        This method will update the instance storage, and also launch the
        instances with the node provider.

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
        requested_instances = []
        allocated_instances = []

        for instance in target_instances.values():
            if instance.status == Instance.QUEUED:
                queued_instances.append(instance)
            elif instance.status == Instance.REQUESTED:
                requested_instances.append(instance)
            else:
                assert InstanceUtil.is_cloud_instance_allocated(instance), (
                    f"Instance {instance.instance_id} has status "
                    f"{instance.status} but is not allocated."
                )
                allocated_instances.append(instance)

        if not queued_instances:
            # This could happen if instances are changed to non-queued status
            # between the time we get instance update and the time we try to
            # to list all queued instances.
            logger.debug("No queued instances to launch.")
            return None

        to_launch = self._get_to_launch(
            queued_instances, requested_instances, allocated_instances
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
        queued_instances: List[Instance],
        requested_instances: List[Instance],
        allocated_instances: List[Instance],
    ) -> Dict[NodeType, List[Instance]]:
        """Returns the number of instances to request for each node type.

        Args:
            queued_instances: the queued instances.
            requested_instances: the requested instances.
            allocated_instances: instances with a cloud instance already
                allocated.

        Returns:
            a dict of instances to request grouped by instance type.
        """

        def _group_by_type(instances):
            instances_by_type = defaultdict(list)
            for instance in instances:
                instances_by_type[instance.instance_type].append(instance)
            return instances_by_type

        # Sort the instances by the time they were queued.
        def _sort_by_earliest_queued(instance: Instance) -> List[int]:
            queue_times = InstanceUtil.get_status_times_ns(instance, Instance.QUEUED)
            return sorted(queue_times)

        queued_instances_by_type = _group_by_type(queued_instances)
        requested_instances_by_type = _group_by_type(requested_instances)
        allocated_instances_by_type = _group_by_type(allocated_instances)

        total_num_requested_to_launch = len(requested_instances)
        all_to_launch = {}

        for (
            instance_type,
            queued_instances_for_type,
        ) in queued_instances_by_type.items():
            requested_instances_for_type = requested_instances_by_type.get(
                instance_type, []
            )
            allocated_instances_for_type = allocated_instances_by_type.get(
                instance_type, []
            )

            num_to_launch = self._compute_num_to_launch_for_type(
                instance_type,
                len(queued_instances_for_type),
                len(requested_instances_for_type),
                len(allocated_instances_for_type),
                total_num_requested_to_launch,
            )

            to_launch = sorted(queued_instances_for_type, key=_sort_by_earliest_queued)[
                :num_to_launch
            ]

            all_to_launch[instance_type] = to_launch
            total_num_requested_to_launch += num_to_launch

        return all_to_launch

    def _compute_num_to_launch_for_type(
        self,
        node_type: NodeType,
        num_queued_for_type: int,
        num_requested_for_type: int,
        num_allocated_for_type: int,
        num_requested_to_launch_for_all: int,
    ) -> int:
        """Compute the number of instances to request for launch.

        This calculates the number of instances to request for launch for a
        specific node type by:
            1. Get the ideal num to launch based on current cluster instances
            for the type.
            2. Cap the number of instances to launch based on the global limit.

        Args:
            node_type: the node type.
            num_queued_for_type: the number of instances that are queued for
                this node type.
            num_requested_for_type: the number of instances that are requested
                for launch for this node type.
            num_allocated_for_type: the number of instances that are already
                allocated for this node type.
            num_requested_to_launch_for_all: the number of instances that are
                requested for launch for all node types.

        Returns:
            the number of instances to request for launch.

        """

        # Compute the desired upscaling for the node type based on current
        # cluster instances.
        num_desired_to_upscale = max(
            1,
            math.ceil(
                self._upscaling_speed
                * (num_requested_for_type + num_allocated_for_type)
            ),
        )

        # Enforce global limit.
        num_to_launch = min(
            self._max_concurrent_launches - num_requested_to_launch_for_all,
            num_desired_to_upscale,
        )

        # Cap both ends 0 <= num_to_launch <= num_queued
        num_to_launch = max(0, num_to_launch)
        num_to_launch = min(num_queued_for_type, num_to_launch)

        logger.debug(
            f"Computed {num_to_launch} instances to launch for node type "
            f"{node_type}. num_queued_for_type={num_queued_for_type}, "
            f"num_requested_for_type={num_requested_for_type}, "
            f"num_allocated_for_type={num_allocated_for_type}, "
            f"num_requested_to_launch_for_all={num_requested_to_launch_for_all},"
            f"max_concurrent_launches={self._max_concurrent_launches}, "
            f"num_desired_to_upscale={num_desired_to_upscale}"
        )

        return num_to_launch
