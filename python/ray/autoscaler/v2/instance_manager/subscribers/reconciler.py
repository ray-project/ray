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
    node provider and instance storage. It is also responsible for handling
    failures.
    """

    def __init__(
        self,
        instance_storage: InstanceStorage,
        node_provider: NodeProvider,
        reconcile_interval_s: int = 120,
    ) -> None:
        self._instance_storage = instance_storage
        self._node_provider = node_provider
        self._failure_handling_executor = ThreadPoolExecutor(max_workers=1)
        self._reconcile_interval_s = reconcile_interval_s
        self._reconcile_timer_lock = threading.Lock()
        with self._reconcile_timer_lock:
            self._reconcile_timer = threading.Timer(
                self._reconcile_interval_s, self._periodic_reconcile_helper
            )

    def shutdown(self):
        with self._reconcile_timer_lock:
            self._reconcile_timer.cancel()

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        instance_ids = [
            event.instance_id
            for event in events
            if event.new_status in {Instance.ALLOCATED}
            and event.new_ray_status
            in {Instance.RAY_STOPPED, Instance.RAY_INSTALL_FAILED}
        ]
        if instance_ids:
            self._failure_handling_executor.submit(
                self._handle_ray_failure, instance_ids
            )

    def _handle_ray_failure(self, instance_ids: List[str]) -> int:
        failing_instances, _ = self._instance_storage.get_instances(
            instance_ids=instance_ids,
            status_filter={Instance.ALLOCATED},
            ray_status_filter={Instance.RAY_STOPPED, Instance.RAY_INSTALL_FAILED},
        )
        if not failing_instances:
            logger.debug("No ray failure")
            return

        failing_instances = failing_instances.values()
        for instance in failing_instances:
            # this call is asynchronous.
            self._node_provider.terminate_node(instance.cloud_instance_id)

            instance.status = Instance.STOPPING
            result, _ = self._instance_storage.upsert_instance(
                instance, expected_instance_version=instance.version
            )
            if not result:
                logger.warning("Failed to update instance status to STOPPING")

    def _periodic_reconcile_helper(self) -> None:
        try:
            self._reconcile_with_node_provider()
        except Exception:
            logger.exception("Failed to reconcile with node provider")
        with self._reconcile_timer_lock:
            self._reconcile_timer = threading.Timer(
                self._reconcile_interval_s, self._periodic_reconcile_helper
            )

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
                instance.status = Instance.STOPPED
                result, _ = self._instance_storage.upsert_instance(
                    instance, expected_instance_version=instance.version
                )
                if not result:
                    logger.warning("Failed to update instance status to STOPPED")

        # 2. TODO: if the cloud instance has no storage instance can be found,
        # it means the instance is likely leaked, terminate the instance.
        # 3. TODO: we should also GC nodes have been stuck in installing state.
