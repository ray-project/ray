import logging
import math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import List

from ray.autoscaler._private.constants import (
    AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
    AUTOSCALER_MAX_LAUNCH_BATCH,
)
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceUtil
from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSubscriber,
)
from ray.autoscaler.v2.instance_manager.node_provider import ICloudNodeProvider
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.core.generated.instance_manager_pb2 import Instance, InstanceUpdateEvent

logger = logging.getLogger(__name__)


class InstanceLauncher(InstanceUpdatedSubscriber):
    """InstanceLauncher is responsible for provisioning new instances."""

    def __init__(
        self,
        instance_storage: InstanceStorage,
        node_provider: ICloudNodeProvider,
        max_concurrent_launches: int = AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
        max_launches_per_request: int = AUTOSCALER_MAX_LAUNCH_BATCH,
    ):
        """
        Args:
            instance_storage: instance storage.
            node_provider: node provider.
            max_concurrent_launches: max number of concurrent launches.
            max_launches_per_request: max number of instances to launch per request.
        """

        self._instance_storage = instance_storage
        self._node_provider = node_provider
        self._max_concurrent_requests = math.ceil(
            max_concurrent_launches / float(max_launches_per_request)
        )

        self._max_launches_per_request = max_launches_per_request
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._launch_instance_executor = ThreadPoolExecutor(
            max_workers=self._max_concurrent_requests
        )

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        """
        For each instance update event, if there is new instance request (signaled by
        Instance.QUEUED status), it will try to launch new instances.

        Args:
            events: list of instance update events.
        """
        has_new_request = any(
            [event.new_instance_status == Instance.QUEUED for event in events]
        )
        if has_new_request:
            self._executor.submit(self._may_launch_new_instances)

    def _may_launch_new_instances(self):
        instances_to_request_launching, _ = self._instance_storage.get_instances(
            status_filter={Instance.QUEUED}
        )

        if not instances_to_request_launching:
            logger.debug("No instances to launch")
            return

        instances_by_type = defaultdict(list)
        for instance in instances_to_request_launching.values():
            instances_by_type[instance.instance_type].append(instance)

        for instance_type, instances in instances_by_type.items():
            # Sort the instances by increasing QUEUED time (oldest first) for
            instances = sorted(
                instances,
                key=lambda x: InstanceUtil.get_status_times_ns(x, Instance.QUEUED)[-1],
            )

            for i in range(0, len(instances), self._max_launches_per_request):
                self._launch_instance_executor.submit(
                    self._launch_new_instances_by_type,
                    instance_type,
                    instances[
                        i : min(
                            i + self._max_launches_per_request,
                            len(instances),
                        )
                    ],
                )

    def _launch_new_instances_by_type(
        self, instance_type: str, instances: List[Instance]
    ) -> int:
        """Launches instances of the given type.

        Args:
            instance_type: type of instance to launch.
            instances: list of instances to launch. These instances should
                have been marked as QUEUED with instance_type set.
        Returns:
            num of instances launched.
        """
        logger.info(f"Launching {len(instances)} instances of type {instance_type}")
        instances_selected = []
        # for instance in instances:
        #     instance.status = Instance.REQUESTED
        #     result, version = self._instance_storage.upsert_instance(
        #         instance, expected_instance_version=instance.version
        #     )
        #     if not result:
        #         logger.warn(f"Failed to update instance {instance}")
        #         continue
        #     instance.version = version
        #     instances_selected.append(instance)

        if not instances_selected:
            return 0

        # TODO: idempotency token.
        created_cloud_instances = self._node_provider.create_nodes(
            instance_type, len(instances_selected)
        )

        assert len(created_cloud_instances) <= len(instances_selected)

        instances_launched = 0
        while created_cloud_instances and instances_selected:
            cloud_instance = created_cloud_instances.pop()
            instance = instances_selected.pop()
            instance.cloud_instance_id = cloud_instance.cloud_instance_id
            instance.internal_ip = cloud_instance.internal_ip
            instance.external_ip = cloud_instance.external_ip
            instance.status = Instance.ALLOCATED

            # update instance status into the storage
            result, _ = self._instance_storage.upsert_instance(
                instance, expected_instance_version=instance.version
            )

            if not result:
                # TODO: this could only happen when the request is canceled.
                logger.warn(f"Failed to update instance {instance}")
                # push the cloud instance back
                created_cloud_instances.append(cloud_instance)
                continue

            instances_launched += 1

        if created_cloud_instances:
            # instances are leaked, we probably need to terminate them
            for instance in created_cloud_instances:
                self._node_provider.terminate_node(instance.cloud_instance_id)

        if instances_selected:
            # instances creation failed, we need to marke them allocation failed.
            for instance in instances_selected:
                instance.status = Instance.ALLOCATION_FAILED
                # TODO: add more information about the failure.
                result, _ = self._instance_storage.upsert_instance(
                    instance, expected_instance_version=instance.version
                )
                # TODO: this could only happen when the request is canceled.
        return instances_launched
