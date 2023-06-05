import logging
import math
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List

from ray.autoscaler._private.constants import (
    AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
    AUTOSCALER_MAX_LAUNCH_BATCH,
)
from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSuscriber,
    InstanceUpdateEvent,
)
from ray.autoscaler.v2.instance_manager.node_provider import NodeProvider
from ray.autoscaler.v2.instance_manager.ray_installer import RayInstaller
from ray.core.generated.instance_manager_pb2 import Instance

logger = logging.getLogger(__name__)


@dataclass
class RayInstallerConfig:
    max_install_attempts: int = 3
    install_retry_interval: int = 10
    max_concurrent_installs: int = 50


@dataclass
class LaunchInstanceConfig:
    max_concurrent_requests: int = math.ceil(
        AUTOSCALER_MAX_CONCURRENT_LAUNCHES / float(AUTOSCALER_MAX_LAUNCH_BATCH)
    )
    max_nodes_per_request: int = AUTOSCALER_MAX_LAUNCH_BATCH


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
        ray_installer: RayInstaller,
        installer_config: RayInstallerConfig = RayInstallerConfig(),
        launch_instance_config: LaunchInstanceConfig = LaunchInstanceConfig(),
    ) -> None:
        self._head_node_ip = head_node_ip
        self._instance_storage = instance_storage
        self._node_provider = node_provider
        self._ray_installer = ray_installer
        self._installer_config = installer_config
        self._launch_instance_config = launch_instance_config
        self._reconciler_executor = ThreadPoolExecutor(max_workers=1)
        self._ray_installation_executor = ThreadPoolExecutor(
            max_workers=self._installer_config.max_concurrent_installs
        )
        self._launch_instance_executor = ThreadPoolExecutor(
            max_workers=self.launch_instance_config.max_concurrent_requests
        )

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        # TODO: we should do reconciliation based on events.
        self._reconciler_executor.submit(self._run_reconcile)

    def _run_reconcile(self) -> None:
        self._launch_new_instances()
        self._install_ray_on_new_nodes()
        self._handle_ray_failure()
        self._reconcile_with_node_provider()

    def _launch_new_instances(self):
        queued_instances, _ = self._instance_storage.get_instances(
            status_filter={Instance.QEUEUD}
        )
        if not queued_instances:
            logger.debug("No queued instances to launch")
            return

        instances_by_type = defaultdict(list)
        instance_type_min_request_time = {}
        for instance in queued_instances.values():
            instances_by_type[instance.instance_type].append(instance)
            instance_type_min_request_time[instance.instance_type] = min(
                instance.timestamp_since_last_modified,
                instance_type_min_request_time.get(
                    instance.instance_type, float("inf")
                ),
            )

        # Sort instance types by the time of the earlest request
        picked_instance_type = sorted(
            instance_type_min_request_time.items(), key=lambda x: x[1]
        )[0][0]

        self._launch_instance_executor.submit(
            self._launch_new_instances_by_type,
            picked_instance_type,
            instances_by_type[picked_instance_type],
        )

    def _launch_new_instances_by_type(
        self, instance_type: str, instances: List[Instance]
    ) -> int:
        logger.info(f"Launching {len(instances)} instances of type {instance_type}")
        instances_selected = []
        for instance in instances:
            instance.status = Instance.REQUESTED
            result, version = self._instance_storage.upsert_instance(
                instance, expected_instance_version=instance.version
            )
            if not result:
                logger.warn(f"Failed to update instance {instance}")
            instance.version = version
            instances_selected.append(instance)
            if (
                len(instances_selected)
                >= self._launch_instance_config.max_nodes_per_request
            ):
                break

        if not instances_selected:
            return 0

        created_cloud_instances = self._node_provider.create_nodes(
            instance_type, len(instances_selected)
        )

        assert len(created_cloud_instances) <= len(instances_selected)

        while created_cloud_instances and instances_selected:
            cloud_instance = created_cloud_instances.pop()
            instance = self._instance_storage.pop()
            instance.cloud_instance_id = cloud_instance.cloud_instance_id
            instance.interal_ip = cloud_instance.internal_ip
            instance.external_ip = cloud_instance.external_ip
            instance.status = Instance.ALLOCATED
            instance.ray_status = Instance.RAY_STATUS_UNKOWN

            # update instance status into the storage
            result, _ = self._instance_storage.upsert_instance(
                instance, expected_instance_version=instance.version
            )

            if not result:
                # TODO: this could only happen when the request is canceled.
                logger.warn(f"Failed to update instance {instance}")
                # push the cloud instance back
                created_cloud_instances.append(cloud_instance)

        if created_cloud_instances:
            # instances are leaked, we probably need to terminate them
            self._node_provider.terminate_nodes(
                [instance.cloud_instance_id for instance in created_cloud_instances]
            )

        if instances_selected:
            # instances creation failed, we need to marke them allocation failed.
            for instance in instances_selected:
                instance.status = Instance.ALLOCATION_FAILED
                result, _ = self._instance_storage.upsert_instance(
                    instance, expected_instance_version=instance.version
                )
                # TODO: this could only happen when the request is canceled.

    def _install_ray_on_new_nodes(self) -> None:
        allocated_instance, _ = self._instance_storage.get_instances(
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
        for _ in range(self._installer_config.max_install_attempts):
            installed = self._ray_installer.install_ray(instance, self._head_node_ip)
            if installed:
                break
            logger.warning("Failed to install ray, retrying...")
            time.sleep(self._installer_config.install_retry_interval * backoff_factor)
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
