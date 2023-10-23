import logging
import math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import List
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceUtil

from ray.autoscaler._private.constants import (
    AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
    AUTOSCALER_MAX_LAUNCH_BATCH,
)
from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSubscriber,
    InstanceUpdateEvent,
)
from ray.autoscaler.v2.instance_manager.node_provider import NodeProvider
from ray.core.generated.instance_manager_pb2 import Instance

logger = logging.getLogger(__name__)


def try_run(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.exception(e)


class InstanceLauncher(InstanceUpdatedSubscriber):
    """InstanceLauncher is responsible for provisioning new instances."""

    def __init__(
        self,
        instance_storage: InstanceStorage,
        node_provider: NodeProvider,
        max_concurrent_requests: int = math.ceil(
            AUTOSCALER_MAX_CONCURRENT_LAUNCHES / float(AUTOSCALER_MAX_LAUNCH_BATCH)
        ),
        max_instances_per_request: int = AUTOSCALER_MAX_LAUNCH_BATCH,
    ) -> None:
        self._instance_storage = instance_storage
        self._node_provider = node_provider
        self._max_concurrent_requests = max_concurrent_requests
        self._max_instances_per_request = max_instances_per_request
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._launch_instance_executor = ThreadPoolExecutor(
            max_workers=self._max_concurrent_requests
        )

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        # TODO: we should do reconciliation based on events.
        has_new_request = any(
            [event.new_instance_status == Instance.UNKNOWN for event in events]
        )
        if has_new_request:
            # TODO: error handling
            self._executor.submit(self._may_launch_new_instances)

    def _may_launch_new_instances(self):
        new_instances, _ = self._instance_storage.get_instances(
            status_filter={Instance.UNKNOWN}
        )

        logger.info(f"new instances to launch: {new_instances}")

        if not new_instances:
            # TODO: who is the inserter of the Instance.UNKNOWN instances?
            logger.debug("No instances to launch")
            return

        queued_instances = []
        for instance in new_instances.values():
            InstanceUtil.set_status(instance, Instance.QUEUED)
            success, version = self._instance_storage.upsert_instance(
                instance, expected_instance_version=instance.version
            )
            if success:
                instance.version = version
                queued_instances.append(instance)
            else:
                logger.error(f"Failed to update {instance} QUEUED")

        instances_by_type = defaultdict(list)
        for instance in queued_instances:
            instances_by_type[instance.instance_type].append(instance)

        # TODO: an actual rate limiting that looks at the number of instances by type
        # requested, and determines if more should be launched.
        for instance_type, instances in instances_by_type.items():
            for i in range(0, len(instances), self._max_instances_per_request):
                # TODO: error handling
                self._launch_instance_executor.submit(
                    try_run,
                    self._launch_new_instances_by_type,
                    instance_type,
                    instances[
                        i : min(
                            i + self._max_instances_per_request,
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
        for instance in instances:
            InstanceUtil.set_status(instance, Instance.REQUESTED)
            result, version = self._instance_storage.upsert_instance(
                instance, expected_instance_version=instance.version
            )
            if not result:
                logger.warn(f"Failed to update instance {instance}")
                continue
            instance.version = version
            instances_selected.append(instance)
        logger.info(f"instance selected: {instances_selected}")
        if not instances_selected:
            logger.info("returning here")
            return 0

        self._node_provider.create_nodes(instance_type, len(instances_selected))
