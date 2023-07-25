import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List

from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSuscriber,
    InstanceUpdateEvent,
)
from ray.autoscaler.v2.instance_manager.ray_installer import RayInstaller
from ray.core.generated.instance_manager_pb2 import Instance

logger = logging.getLogger(__name__)


class ThreadedRayInstaller(InstanceUpdatedSuscriber):
    """ThreadedRayInstaller is responsible for install ray on new nodes."""

    def __init__(
        self,
        head_node_ip: str,
        instance_storage: InstanceStorage,
        ray_installer: RayInstaller,
        max_install_attempts: int = 3,
        install_retry_interval: int = 10,
        max_concurrent_installs: int = 50,
    ) -> None:
        self._head_node_ip = head_node_ip
        self._instance_storage = instance_storage
        self._ray_installer = ray_installer
        self._max_concurrent_installs = max_concurrent_installs
        self._max_install_attempts = max_install_attempts
        self._install_retry_interval = install_retry_interval
        self._ray_installation_executor = ThreadPoolExecutor(
            max_workers=self._max_concurrent_installs
        )

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        for event in events:
            if (
                event.new_status == Instance.ALLOCATED
                and event.new_ray_status == Instance.RAY_STATUS_UNKOWN
            ):
                self._install_ray_on_new_nodes(event.instance_id)

    def _install_ray_on_new_nodes(self, instance_id: str) -> None:
        allocated_instance, _ = self._instance_storage.get_instances(
            instance_ids={instance_id},
            status_filter={Instance.ALLOCATED},
            ray_status_filter={Instance.RAY_STATUS_UNKOWN},
        )
        for instance in allocated_instance.values():
            self._ray_installation_executor.submit(
                self._install_ray_on_single_node, instance
            )

    def _install_ray_on_single_node(self, instance: Instance) -> None:
        assert instance.status == Instance.ALLOCATED
        assert instance.ray_status == Instance.RAY_STATUS_UNKOWN
        instance.ray_status = Instance.RAY_INSTALLING
        success, version = self._instance_storage.upsert_instance(
            instance, expected_instance_version=instance.version
        )
        if not success:
            logger.warning(
                f"Failed to update instance {instance.instance_id} to RAY_INSTALLING"
            )
            # Do not need to handle failures, it will be covered by
            # garbage collection.
            return

        # install with exponential backoff
        installed = False
        backoff_factor = 1
        for _ in range(self._max_install_attempts):
            installed = self._ray_installer.install_ray(instance, self._head_node_ip)
            if installed:
                break
            logger.warning("Failed to install ray, retrying...")
            time.sleep(self._install_retry_interval * backoff_factor)
            backoff_factor *= 2

        if not installed:
            instance.ray_status = Instance.RAY_INSTALL_FAILED
            success, version = self._instance_storage.upsert_instance(
                instance,
                expected_instance_version=version,
            )
        else:
            instance.ray_status = Instance.RAY_RUNNING
            success, version = self._instance_storage.upsert_instance(
                instance,
                expected_instance_version=version,
            )
        if not success:
            logger.warning(
                f"Failed to update instance {instance.instance_id} to {instance.status}"
            )
            # Do not need to handle failures, it will be covered by
            # garbage collection.
            return
