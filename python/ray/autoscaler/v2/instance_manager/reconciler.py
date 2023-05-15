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


PENDING_INSTANCE_STATUS = {Instance.INSTANCE_STATUS_UNSPECIFIED}


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
        self._reconcile_with_node_provider()

    def _launch_new_instances(self):
        queued_instances, storage_version = self._instance_storage.get_instances(
            status_filter={Instance.INSTANCE_STATUS_UNSPECIFIED}
        )
        if not queued_instances:
            logger.info("No queued instances to launch")
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
        self, instance_type: str, instances: List[Instance], storage_version
    ) -> int:
        logger.info(f"Launching {len(instances)} instances of type {instance_type}")
        for instance in instances:
            instance.state = Instance.STARTING

        result, version = self._instance_storage.upsert_instances(
            instances, storage_version
        )
        if not result:
            # TODO: we can retry here if starved.
            logger.info(
                f"Failed to update instance status due to version mismatch, "
                f"expected version {storage_version}, actual version {version}"
            )
            return 0

        created_cloud_instances = self._node_provider.create_nodes(
            instance_type, len(instances)
        )

        assert len(created_cloud_instances) <= len(instances)

        for i, instance in enumerate(instances):
            if i < len(created_cloud_instances):
                cloud_instance = created_cloud_instances[i]
                instance.cloud_instance_id = cloud_instance.cloud_instance_id
                instance.interal_ip = cloud_instance.internal_ip
                instance.external_ip = cloud_instance.external_ip
                instance.state = Instance.INSTANCE_ALLOCATED
            else:
                # alternatively, we can retry.
                # TODO: we probably should retry here anyway.
                instance.state = Instance.ALLOCATION_FAILED

        # blind upsert that ignores version mismatch
        self._instance_storage.upsert_instances(instances, expected_version=None)

    def _install_ray_on_new_nodes(self) -> None:
        allocated_instance, storage_version = self._instance_storage.get_instances(
            status_filter={Instance.INSTANCE_ALLOCATED}
        )
        for instance in allocated_instance.values():
            self._ray_installaion_executor.submit(
                self._install_ray_on_single_node, instance
            )

    def _install_ray_on_single_node(self, instance: Instance) -> None:
        instance.state = Instance.RAY_INSTALLING
        self._instance_storage.upsert_instances([instance], expected_version=None)
        try:
            self._ray_installer.install_ray_on_node(instance, self._head_node_ip)
        except Exception:
            instance.state = Instance.RAY_INSTALL_FAILED
            self._instance_storage.upsert_instances([instance], expected_version=None)
            raise
        instance.state = Instance.RUNNING
        self._instance_storage.upsert_instances([instance], expected_version=None)

    def _terminate_failing_nodes(self) -> int:
        failling_instances, storage_version = self._instance_storage.get_instances(
            status_filter={Instance.FAILING}
        )
        if not failling_instances:
            logger.info("No instances to terminate")
            return

        for instance in failling_instances:
            instance.state = Instance.STOPPING

        result, version = self._instance_storage.upsert_instances(
            failling_instances, expected_version=storage_version
        )

        if not result:
            # TODO: we should retry here if verson conflicts.
            logger.info(
                f"Failed to update instance status due to version mismatch, "
                f"expected version {storage_version}, actual version {version}"
            )
            return 0

        cloud_instance_ids = [
            instance.cloud_instance_id for instance in failling_instances
        ]
        self._node_provider.async_terminate_nodes(cloud_instance_ids)
        return len(cloud_instance_ids)

    def _terminate_stopping_nodes(self) -> int:
        # TODO: we should retry nodes that are stuck in STOPPING state.
        pass

    def _reconcile_with_node_provider(self) -> None:
        none_terminated_cloud_instances = self._node_provider.get_non_terminated_nodes()
        instances, _ = self._instance_storage.get_instances()
        for instance in instances.values():
            self._reconcile_single_intance(instance, none_terminated_cloud_instances)

        # dealing with leaked intances.
        self._reconcile_leaked_instances(none_terminated_cloud_instances)

    def _reconcile_single_intance(
        self, storage_instance: Instance, cloud_instance_by_id: Dict[str, Instance]
    ) -> None:
        """Reconcile the instance state with the cloud instance state.

        Args:
            storage_instance: The instance in the storage.
            cloud_instance_by_id: A dictionary of cloud instances keyed by
            cloud_instance_id.
        """
        # if the instance doesn't have cloud_instance_id, it means it is not
        # launched yet, or it is already terminated.
        if not storage_instance.cloud_instance_id:
            assert storage_instance.state in PENDING_INSTANCE_STATUS
            return

        cloud_instance = cloud_instance_by_id.pop(
            storage_instance.cloud_instance_id, None
        )
        # if the cloud instance is not found, it means the instance is already
        if cloud_instance is None:
            logging.info(f"Instance is not found in cloud provider, {storage_instance}")
            # TODO: we should log error if the instance is not expected to
            # be disappeared.
            self._instance_storage.batch_delete_instances(
                [storage_instance.instance_id]
            )
            return

    def _reconcile_leaked_instances(self, potentially_leaked_instances: List[Instance]):
        # TODO: after reconciling with the storage, the remaining cloud instances
        # could either be leaked, or they are just launched and not
        # stored into the storage yet.
        #
        # We are not able to tell the difference between these two cases.
        pass
