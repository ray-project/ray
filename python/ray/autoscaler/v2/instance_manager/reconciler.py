import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSuscriber,
    InstanceUpdateEvent,
)
from ray.autoscaler.v2.instance_manager.node_provider import NodeProvider
from ray.autoscaler.v2.instance_manager.ray_installer import RayInstaller
from ray.core.generated.instance_manager_pb2 import Instance

logger = logging.getLogger(__name__)


PENDING_START_STATUS = {Instance.QEUEUD}
STARTING_STATUS = {Instance.QEUEUD}
ALIVE_STATUS = {Instance.QEUEUD}
STOPPING_STATUS = {Instance.QEUEUD}
STOPPED_STATUS = {Instance.QEUEUD}


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
    ) -> None:
        self._head_node_ip = head_node_ip
        self._instance_storage = instance_storage
        self._node_provider = node_provider
        self._ray_installer = ray_installer
        self._reconciler_executor = ThreadPoolExecutor(max_workers=1)
        self._ray_installaion_executor = ThreadPoolExecutor(max_workers=50)

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        # TODO: we should do reconciliation based on events.
        self._reconciler_executor.submit(self._run_reconcile)

    def _run_reconcile(self) -> None:
        self._install_ray_on_new_nodes()
        self._launch_new_instances()
        self._install_ray_on_new_nodes()
        self._terminate_failing_nodes()
        self._reconcile_leaked_instances()

    def _launch_new_instances(self):
        queued_instances, storage_version = self._instance_storage.get_instances(
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
                instance.timestamp_since_last_state_change,
                instance_type_min_request_time.get(
                    instance.instance_type, float("inf")
                ),
            )

        # Sort instance types by the time of the earlest request
        picked_instance_type = sorted(
            instance_type_min_request_time.items(), key=lambda x: x[1]
        )[0][0]
        self._launch_new_instances_by_type(
            picked_instance_type,
            instances_by_type[picked_instance_type],
            storage_version,
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
                logger.warn(
                    f"Failed to update instance {instance} status due to version mismatch, "
                    "it is likely that the instance has been updated by others."
                )
            instance.version = version
            instances_selected.append(instance)

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
                logger.warn(
                    f"Failed to update instance {instance} status due to version mismatch, "
                    "it is likely that the instance has been updated by others."
                )
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
            status_filter={Instance.ALLOCATED}
        )
        for instance in allocated_instance.values():
            if instance.ray_status == Instance.RAY_STATUS_UNKOWN:
                self._ray_installaion_executor.submit(
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

        if not self._ray_installer.install_ray(instance, self._head_node_ip):
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

    def _terminate_failing_nodes(self) -> int:
        failing_instances, _ = self._instance_storage.get_instances(
            status_filter={Instance.ALLOCATED},
            ray_status_filter={Instance.RAY_INSTALL_FAILED},
        )
        if not failing_instances:
            logger.info("No instances to terminate")
            return

        failing_instances = failing_instances.values()
        cloud_instance_ids = [
            instance.cloud_instance_id for instance in failing_instances
        ]

        self._node_provider.async_terminate_nodes(cloud_instance_ids)

        for instance in failing_instances:
            instance.status = Instance.STOPPING
            self._instance_storage.upsert_instance(
                instance, expected_instance_version=instance.version
            )

        return len(cloud_instance_ids)

    def _reconcile_with_node_provider(self) -> None:
        # main reconcile loop.
        #
        # 1. reconcile the difference between cloud provider and storage.
        # 2. launch new instances.
        # 3. detect leaked instances.
        none_terminated_cloud_instances = self._node_provider.get_non_terminated_nodes()
        instances, _ = self._instance_storage.get_instances()
        for instance in instances.values():
            self._reconcile_single_intance(instance, none_terminated_cloud_instances)

    def _reconcile_single_intance(
        self,
        storage_instance: Instance,
        none_terminated_cloud_instance_by_id: Dict[str, Instance],
    ) -> None:
        """Reconcile the instance state with the cloud instance state.

        Args:
            storage_instance: The instance in the storage.
            cloud_instance_by_id: A dictionary of cloud instances keyed by
            cloud_instance_id.
        """
        # if the instance doesn't have cloud_instance_id, it means it is either not
        # launched yet, or it is already being terminated.
        if not storage_instance.cloud_instance_id:
            assert storage_instance.status in PENDING_START_STATUS
            return

        cloud_instance = none_terminated_cloud_instance_by_id.pop(
            storage_instance.cloud_instance_id, None
        )
        # if the cloud instance is not found, it means the instance is already being
        # terminated.
        if cloud_instance is None:
            logging.info(f"Instance is not found in cloud provider, {storage_instance}")

            assert storage_instance.status not in PENDING_START_STATUS

            # TODO: we should log error if the instance is not expected to
            # be disappeared.
            self._instance_storage.batch_delete_instances(
                [storage_instance.instance_id]
            )

    def _reconcile_leaked_instances(self):
        # TODO: after reconciling with the storage, the remaining cloud instances
        # could either be leaked, or they are just launched and not
        # stored into the storage yet.
        #
        # We are not able to tell the difference between these two cases.
        pass
