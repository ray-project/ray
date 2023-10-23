from collections import defaultdict
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
import time
from typing import Dict, List, Set
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_UNMANAGED,
    TAG_RAY_NODE_KIND,
)
from ray.autoscaler.v2.schema import (
    DEFAULT_INSTANCE_REQUEST_TIMEOUT_S,
    DEFAULT_RECONCILE_INTERVAL_S,
    CloudInstanceId,
    InstanceId,
)
from ray.autoscaler.v2.instance_manager.instance_manager import (
    InstanceUtil,
    assign_instance_to_cloud_node,
    get_status_time_s,
)

from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSubscriber,
    InstanceUpdateEvent,
)
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudProviderNode,
    CloudProviderNode,
    NodeProvider,
)
from ray.core.generated.instance_manager_pb2 import Instance

logger = logging.getLogger(__name__)


class InstanceReconciler(InstanceUpdatedSubscriber):
    """InstanceReconciler is responsible for reconciling the difference between
    node provider and instance storage. It is also responsible for handling
    failures.
    """

    def __init__(
        self,
        instance_storage: InstanceStorage,
        node_provider: NodeProvider,
        reconcile_interval_s: int = DEFAULT_RECONCILE_INTERVAL_S,
    ) -> None:
        self._instance_storage = instance_storage
        self._node_provider = node_provider
        # TODO: rickyx: we should probably use a single thread executor here, and use
        self._failure_handling_executor = ThreadPoolExecutor(max_workers=1)
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._reconcile_interval_s = reconcile_interval_s
        self._reconcile_timer_lock = threading.Lock()
        self._started = False

    def start(self):
        if self._started:
            raise RuntimeError("InstanceReconciler already started")

        with self._reconcile_timer_lock:
            self._started = True
            self._reconcile_timer = threading.Timer(
                self._reconcile_interval_s, self._periodic_reconcile_helper
            )
            self._reconcile_timer.start()

    def shutdown(self):
        with self._reconcile_timer_lock:
            self._started = False
            self._reconcile_timer.cancel()

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        self._failure_handling_executor.submit(self._handle_ray_failure_events, events)

    def _handle_ray_failure_events(self, events: List[InstanceUpdateEvent]) -> int:
        instance_ids = [
            event.instance_id
            for event in events
            if event.new_ray_status
            in {Instance.RAY_STOPPED, Instance.RAY_INSTALL_FAILED}
        ]

        if not instance_ids:
            return

        ray_failed_instances, _ = self._instance_storage.get_instances(
            instance_ids=instance_ids
        )

        assert len(ray_failed_instances) == len(instance_ids)
        for ray_failed_instance in ray_failed_instances.values():
            assert (
                ray_failed_instance.status == Instance.ALLOCATED
            ), "Instance should be in allocated state when ray failure events happen"

            self._node_provider.terminate_node(ray_failed_instance.cloud_instance_id)

            ray_failed_instance.status = Instance.STOPPING
            InstanceUtil.set_status(
                ray_failed_instance,
                new_instance_status=Instance.STOPPING,
            )
            result, _ = self._instance_storage.upsert_instance(
                ray_failed_instance,
                expected_instance_version=ray_failed_instance.version,
            )
            if not result:
                logger.warning(
                    f"Failed to update instance status of {ray_failed_instance.instance_id} to STOPPING"
                )

    def _periodic_reconcile_helper(self) -> None:
        try:
            self._reconcile_with_node_provider()
        except Exception:
            # TODO: error handling.
            logger.exception("Failed to reconcile with node provider")
        with self._reconcile_timer_lock:
            self._reconcile_timer = threading.Timer(
                self._reconcile_interval_s, self._periodic_reconcile_helper
            )
            self._reconcile_timer.start()

    def _reconcile_with_node_provider(self) -> None:
        """
        A few things that we need to reconcile for node providers by polling
        the non-terminated instances from the node provider. These are all
        required state transitions that are not triggered by any instance
        state change events.

        1. The instance might have finished launching, but the instance storage
        is still in Instance.REQUESTED state. We need to change it to
        Instance.ALLOCATED.
            - This happens since some node providers are asynchronous, and we
            need to wait for the instance to be launched.
            - Check the node_provider.launch_mode() to see if the node
            provider is synchronous or asynchronous.

        2. The instance might have been terminated/deleted by the node provider,
        but the instance storage is still in Instance.ALLOCATED state. We need
        to change it to Instance.STOPPED.
            - This happens since some node providers would shutdown the node when
            ray fails or if the node fails unexpectedly.

        3. The instance was being stopped (Instance.STOPPING), and it's now
        terminated. We will need to mark it as Instance.STOPPED.

        TODO:
        4. Any stopped instances that have been stopped for a while?

        """

        # reconcile storage state with cloud state.
        running_cloud_node_ids: List[
            CloudInstanceId
        ] = self._node_provider.get_non_terminated_nodes()

        logger.info(f"Running cloud nodes: {running_cloud_node_ids}")

        unassigned_cloud_nodes: List[
            CloudProviderNode
        ] = self._reconcile_allocated_nodes(running_cloud_node_ids)
        self._terminate_leak_cloud_nodes(
            [node.cloud_instance_id for node in unassigned_cloud_nodes]
        )

        # TODO: we should also track the time each node has been stuck at
        # certain state, and take action if it's stuck for too long?
        # e.g.
        # 1. if the node is stuck at launching for too long, we should
        # fail the launching.
        # 2. if the node is stuck at stopping for too long, we probably cannot do
        # much, other than warnings
        # 3. if the node is stuck ray status change? (maybe just show)
        self._reconcile_terminated_nodes(set(running_cloud_node_ids))
        self._reconcile_requesting_nodes()

    def _reconcile_requesting_nodes(self):
        instances, _ = self._instance_storage.get_instances(
            status_filter={Instance.REQUESTED}
        )

        for i in instances.values():
            request_time_s = get_status_time_s(
                i, instance_status=Instance.REQUESTED, reverse=True
            )
            assert (
                request_time_s is not None
            ), f"Status history is corrupted for instance: {i}"
            assert (
                time.time() > request_time_s
            ), f"Invalid request time {request_time_s} > {time.time()}. Time traveled?"

            if time.time() - request_time_s > DEFAULT_INSTANCE_REQUEST_TIMEOUT_S:
                # We have been waiting for the node to launch for too long. Try queuing this again.
                # TODO: We should probably revisit this logic when we have rate limiting in-placed.
                InstanceUtil.set_status(
                    i,
                    new_instance_status=Instance.UNKNOWN,
                )
                result, _ = self._instance_storage.upsert_instance(
                    i, expected_instance_version=i.version
                )
                if not result:
                    logger.error(
                        f"Failed to update instance {i.instance_id} status to QUEUED"
                    )
            else:
                # Do nothing, we will wait for the node to launch.
                pass

    def _terminate_leak_cloud_nodes(self, leaked_cloud_node_ids: List[CloudInstanceId]):
        if len(leaked_cloud_node_ids) == 0:
            return

        logger.error(
            "{} cloud instances are leaked, terminating them: {}".format(
                len(leaked_cloud_node_ids), leaked_cloud_node_ids
            )
        )

        for cloud_node_id in leaked_cloud_node_ids:
            # Async terminate the node.
            self._node_provider.terminate_node(cloud_node_id)

    def _reconcile_allocated_nodes(
        self, running_cloud_node_ids_ids: List[CloudInstanceId]
    ):
        """
        if the storage instance is in REQUESTED state and the cloud
        instance is found, change the instance state to ALLOCATED.
        """
        instances, _ = self._instance_storage.get_instances()
        all_assigned_cloud_instance_ids = {
            instance.cloud_instance_id for instance in instances.values()
        }

        to_assign_cloud_instances: List[CloudProviderNode] = []
        for running_cloud_instance_id in running_cloud_node_ids_ids:
            if running_cloud_instance_id in all_assigned_cloud_instance_ids:
                # The cloud instance is already assigned.
                continue
            to_assign_cloud_instances.append(
                self._node_provider.get_node_info_by_id(running_cloud_instance_id)
            )

        requested_instance_by_type: Dict[str, List[Instance]] = defaultdict(list)
        for instance in instances.values():
            if instance.status == Instance.REQUESTED:
                requested_instance_by_type[instance.instance_type].append(instance)

        unassigned_cloud_instances = []
        logger.info("to assigned cloud instances: {}".format(to_assign_cloud_instances))
        logger.info(
            "requested instances by type: {}".format(requested_instance_by_type)
        )
        for cloud_node in to_assign_cloud_instances:
            instance_type = cloud_node.instance_type

            if len(requested_instance_by_type[instance_type]) == 0:
                # No instance requested for this instance type? This is
                # unexpected. But will be handled later.
                unassigned_cloud_instances.append(cloud_node)
                continue

            unassigned_instance = requested_instance_by_type[instance_type].pop()
            assign_instance_to_cloud_node(unassigned_instance, cloud_node)
            # Add to storage
            result, _ = self._instance_storage.upsert_instance(
                unassigned_instance,
                expected_instance_version=unassigned_instance.version,
            )
            if not result:
                logger.error(
                    f"Failed to update instance {instance.instance_id} status to ALLOCATED"
                )

        # TODO: what kind of nodes we allow for having a non-matching instance?
        # Filter out head node cloud instances
        return [
            i
            for i in unassigned_cloud_instances
            if i.node_tags.get(TAG_RAY_NODE_KIND, NODE_KIND_UNMANAGED) != NODE_KIND_HEAD
        ]

    def _reconcile_terminated_nodes(self, running_cloud_node_ids: Set[CloudInstanceId]):
        """
        If any allocated/stopping instance is not found in the running_cloud_instance,
        they are already terminated by the cloud provider. We will need to mark it as stopped.
        """
        allocated_instances, _ = self._instance_storage.get_instances(
            status_filter={Instance.ALLOCATED, Instance.STOPPING}
        )

        for instance in allocated_instances.values():
            if instance.cloud_instance_id in running_cloud_node_ids:
                # The allocated instance is still running.
                continue

            InstanceUtil.set_status(
                instance,
                new_instance_status=Instance.STOPPED,
                new_ray_status=Instance.RAY_STATUS_UNKNOWN,
            )
            result, _ = self._instance_storage.upsert_instance(
                instance, expected_instance_version=instance.version
            )
            if not result:
                logger.error(
                    f"Failed to update instance {instance.instance_id} status to STOPPED"
                )
