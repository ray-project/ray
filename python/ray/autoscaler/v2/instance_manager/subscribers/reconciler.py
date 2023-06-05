import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List

from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSuscriber,
    InstanceUpdateEvent,
)
from ray.autoscaler.v2.instance_manager.node_provider import NodeProvider
from ray.core.generated.instance_manager_pb2 import Instance

logger = logging.getLogger(__name__)


class InstanceReconciler(InstanceUpdatedSuscriber):
    """InstanceReconciler is responsible for reconciling the difference between
    node provider and instance storage. It is also responsible for launching new
    nodes and terminating failing nodes.
    """

    def __init__(
        self,
        head_node_ip: str,
        instance_storage: InstanceStorage,
        node_provider: NodeProvider,
        reconciler_interval_s: int = 120,
    ) -> None:
        self._head_node_ip = head_node_ip
        self._instance_storage = instance_storage
        self._node_provider = node_provider
        self._failure_handling_executor = ThreadPoolExecutor(max_workers=1)
        self._reconcile_timer = threading.Timer(
            reconciler_interval_s, self._reconcile_with_node_provider
        )
        self._reconcile_timer.start()

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        if any(
            event.new_status in [Instance.ALLOCATED]
            and event.new_ray_status
            in [Instance.RAY_STOPPED, Instance.RAY_INSTALL_FAILED]
            for event in events
        ):
            self._failure_handling_executor.submit(self._handle_ray_failure)

    def _handle_ray_failure(self) -> int:
        failing_instances, _ = self._instance_storage.get_instances(
            status_filter={Instance.ALLOCATED},
            ray_status_filter={Instance.RAY_STOPPED, Instance.RAY_INSTALL_FAILED},
        )
        if not failing_instances:
            logger.info("No ray failure")
            return

        failing_instances = failing_instances.values()
        cloud_instance_ids = [
            instance.cloud_instance_id for instance in failing_instances
        ]

        self._node_provider.async_terminate_nodes(cloud_instance_ids)

        for instance in failing_instances:
            instance.status = Instance.STOPPING
            result, _ = self._instance_storage.upsert_instance(
                instance, expected_instance_version=instance.version
            )
            if not result:
                logger.warning("Failed to update instance status to STOPPING")

        return len(cloud_instance_ids)

    def _reconcile_with_node_provider(self) -> None:
        # reconcile storage state with cloud state.
        none_terminated_cloud_instances = self._node_provider.get_non_terminated_nodes()

        # 1. if the storage instance is in STOPPING state and the no
        # cloud instance is found, change the instance state to TERMINATED.
        stopping_instances, _ = self._instance_storage.get_instances(
            status_filter={Instance.STOPPING}
        )

        for instance in stopping_instances.values():
            if none_terminated_cloud_instances.get(instance.cloud_instance_id) is None:
                instance.status = Instance.TERMINATED
                result, _ = self._instance_storage.upsert_instance(
                    instance, expected_instance_version=instance.version
                )
                if not result:
                    logger.warning("Failed to update instance status to TERMINATED")

        # 2. if the cloud instance has no storage instance can be found,
        # it means the instance is likely leaked, terminate the instance.
        # TODO
