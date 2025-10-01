import dataclasses
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List
from queue import Queue

from ray.autoscaler.v2.instance_manager.instance_manager import (
    InstanceUpdatedSubscriber,
)
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.ray_installer import RayInstaller
from ray.core.generated.instance_manager_pb2 import (
    NodeKind,
    Instance,
    InstanceUpdateEvent,
)

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class RayInstallError:
    # Instance manager's instance id.
    im_instance_id: str
    # Error details.
    details: str


class ThreadedRayInstaller(InstanceUpdatedSubscriber):
    """ThreadedRayInstaller is responsible for install ray on new nodes."""

    def __init__(
        self,
        head_node_ip: str,
        instance_storage: InstanceStorage,
        ray_installer: RayInstaller,
        error_queue: Queue,
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
        self._error_queue = error_queue
        self._ray_installation_executor = ThreadPoolExecutor(
            max_workers=self._max_concurrent_installs
        )

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        for event in events:
            if event.new_instance_status == Instance.RAY_INSTALLING:
                self._install_ray_on_new_nodes(event.instance_id)

    def _install_ray_on_new_nodes(self, instance_id: str) -> None:
        allocated_instance, _ = self._instance_storage.get_instances(
            instance_ids={instance_id},
            status_filter={Instance.RAY_INSTALLING},
        )
        for instance in allocated_instance.values():
            assert instance.node_kind == NodeKind.WORKER
            self._ray_installation_executor.submit(
                self._install_ray_on_single_node, instance
            )

    def _install_ray_on_single_node(self, instance: Instance) -> None:
        assert instance.status == Instance.RAY_INSTALLING

        # install with exponential backoff
        backoff_factor = 1
        last_exception = None
        for _ in range(self._max_install_attempts):
            try:
                self._ray_installer.install_ray(instance, self._head_node_ip)
                return
            except Exception as e:
                logger.info(
                    f"Ray installation failed on instance {instance.cloud_instance_id}: {e}"
                )
                last_exception = e

            logger.warning("Failed to install ray, retrying...")
            time.sleep(self._install_retry_interval * backoff_factor)
            backoff_factor *= 2

        self._error_queue.put_nowait(
            RayInstallError(
                im_instance_id=instance.instance_id,
                details=str(last_exception),
            )
        )
