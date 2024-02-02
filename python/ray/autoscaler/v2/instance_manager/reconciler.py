import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from google.protobuf.json_format import MessageToDict

from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.autoscaler.v2.instance_manager.config import InstanceReconcileConfig
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    CloudInstanceId,
    CloudInstanceProviderError,
    LaunchNodeError,
)
from ray.autoscaler.v2.instance_manager.ray_installer import RayInstallError
from ray.autoscaler.v2.scheduler import IResourceScheduler
from ray.core.generated.autoscaler_pb2 import ClusterResourceState, NodeState
from ray.core.generated.instance_manager_pb2 import GetInstanceManagerStateRequest
from ray.core.generated.instance_manager_pb2 import Instance as IMInstance
from ray.core.generated.instance_manager_pb2 import (
    InstanceUpdateEvent as IMInstanceUpdateEvent,
)
from ray.core.generated.instance_manager_pb2 import (
    StatusCode,
    UpdateInstanceManagerStateRequest,
)

logger = logging.getLogger(__name__)


class Reconciler:
    """
    Reconciler is responsible for
        1. Reconciling the instance manager's instances with external states like
        the cloud provider's, the ray cluster's states, the ray installer's results.
        It performs "passive" status transitions for the instances (where the status
        transition should only be reflecting the external states of the cloud provider
        and the ray cluster, and should not be actively changing them)

        2. Stepping the reconciler to the next state by computing instance status
        transitions that are needed and updating the instance manager's state.
        These transitions should be "active" where the transitions have side effects
        (through InstanceStatusSubscriber) to the cloud provider and the ray cluster.

    Example:
    ```
        # Step 1: Reconcile the instance manager's instances with external states.
        Reconciler.sync_from([external states])

        # Step 2: Step the reconciler to the next state by computing instance status
        # transitions that are needed and updating the instance manager's state.
        Reconciler.step_next()

    """

    @staticmethod
    def sync_from(
        instance_manager: InstanceManager,
        ray_nodes: List[NodeState],
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
        cloud_provider_errors: List[CloudInstanceProviderError],
        ray_install_errors: List[RayInstallError],
        config: InstanceReconcileConfig,
    ):
        """
        Reconcile the instance states of the instance manager from external states like
        the cloud provider's, the ray cluster's states, the ray installer's results,
        etc.

        For each instance, we try to figure out if we need to transition the instance
        status to a new status, and if so, what the new status should be.

        These transitions should be purely "passive", meaning they should only be
        reflecting the external states of the cloud provider and the ray cluster,
        and should not be actively changing the states of the cloud provider or the ray
        cluster.

        More specifically, we will reconcile status transitions for:
            1.  QUEUED/REQUESTED -> ALLOCATED:
                When a instance with launch request id (indicating a previous launch
                request was made) could be assigned to an unassigned cloud instance
                of the same instance type.
            2.  REQUESTED -> ALLOCATION_FAILED/QUEUED:
                When there's an error from the cloud provider for launch failure so
                that the instance becomes ALLOCATION_FAILED, or the requested instance
                is not assigned for too long that it times out and becomes QUEUED
                again.
            3.  * -> RAY_RUNNING:
                When a ray node on a cloud instance joins the ray cluster, we will
                transition the instance to RAY_RUNNING.
            4.  * -> TERMINATED:
                When the cloud instance is already terminated, we will transition the
                instance to TERMINATED.
            5.  TERMINATING -> TERMINATION_FAILED:
                When there's an error from the cloud provider for termination failure.
            6.  * -> RAY_STOPPED:
                When ray was stopped on the cloud instance, we will transition the
                instance to RAY_STOPPED.
            7.  * -> RAY_INSTALL_FAILED:
                When there's an error from RayInstaller.

        Args:
            instance_manager: The instance manager to reconcile.
            ray_nodes: The ray cluster's states of ray nodes.
            non_terminated_cloud_instances: The non-terminated cloud instances from
                the cloud provider.
            cloud_provider_errors: The errors from the cloud provider.
            ray_install_errors: The errors from RayInstaller.
            config: The config for the instance reconcile.

        """

        # Handle 1 & 2 for cloud instance allocation.
        Reconciler._handle_cloud_instance_allocation(
            instance_manager,
            non_terminated_cloud_instances,
            cloud_provider_errors,
            config,
        )
        Reconciler._handle_cloud_instance_terminated(
            instance_manager, non_terminated_cloud_instances, cloud_provider_errors
        )
        Reconciler._handle_ray_running(instance_manager, ray_nodes)
        Reconciler._handle_ray_stopped(instance_manager, ray_nodes)
        Reconciler._handle_ray_install_failed(instance_manager, ray_install_errors)

    @staticmethod
    def step_next(
        instance_manager: InstanceManager,
        ray_cluster_resource_state: ClusterResourceState,
        scheduler: IResourceScheduler,
        config: InstanceReconcileConfig,
    ):
        """
        Step the reconciler to the next state by computing instance status transitions
        that are needed and updating the instance manager's state.

        Specifically, we will:
            1. Shut down extra cloud instances
              (* -> TERMINATING)
                a. Leaked cloud instances that are not managed by the instance manager.
                b. Extra cloud due to max nodes config.
                c. Cloud instances with outdated configs.
                d. Stopped ray nodes or failed to install ray nodes.
            2. Create new instances
              (new QUEUED)
                Create new instances based on the IResourceScheduler's decision for
                scaling up.
            3. Request cloud provider to launch new instances.
              (QUEUED -> REQUESTED)
            4. Install ray
              (ALLOCATED -> RAY_INSTALLING)
                When ray needs to be manually installed.
            5. Drain ray nodes
              (RAY_RUNNING -> RAY_STOPPING):
                a. Idle terminating ray nodes.
        """
        pass

    @staticmethod
    def _handle_cloud_instance_allocation(
        instance_manager: InstanceManager,
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
        cloud_provider_errors: List[CloudInstanceProviderError],
        config: InstanceReconcileConfig,
    ):
        im_instances, version = Reconciler._get_im_instances(instance_manager)
        updates = {}

        # Compute intermediate states.
        instances_with_launch_requests: List[IMInstance] = [
            instance for instance in im_instances if instance.launch_request_id
        ]
        assigned_cloud_instance_ids: Set[CloudInstanceId] = {
            instance.cloud_instance_id for instance in im_instances
        }
        launch_errors: Dict[str, LaunchNodeError] = {
            error.request_id: error
            for error in cloud_provider_errors
            if isinstance(error, LaunchNodeError)
        }
        unassigned_cloud_instances_by_type: Dict[
            str, List[CloudInstance]
        ] = defaultdict(list)

        for cloud_instance_id, cloud_instance in non_terminated_cloud_instances.items():
            if cloud_instance_id not in assigned_cloud_instance_ids:
                unassigned_cloud_instances_by_type[cloud_instance.node_type].append(
                    cloud_instance
                )

        # Sort the request instance by the increasing request time.
        instances_with_launch_requests.sort(
            key=lambda instance: InstanceUtil.get_status_transition_times_ns(
                instance, IMInstance.REQUESTED
            )
        )

        # For each instance, try to allocate or fail the allocation.
        def _allocate_or_fail(im_instance) -> Optional[IMInstanceUpdateEvent]:
            unassigned_cloud_instance = None

            # Try to allocate an unassigned cloud instance.
            # TODO(rickyx): We could also look at the launch request id
            # on the cloud node and the im instance later once all node providers
            # support request id. For now, we only look at the instance type.
            if len(
                unassigned_cloud_instances_by_type.get(im_instance.instance_type, [])
            ):
                unassigned_cloud_instance = unassigned_cloud_instances_by_type[
                    im_instance.instance_type
                ].pop()

            if unassigned_cloud_instance:
                return IMInstanceUpdateEvent(
                    instance_id=im_instance.instance_id,
                    new_instance_status=IMInstance.ALLOCATED,
                    cloud_instance_id=unassigned_cloud_instance.cloud_instance_id,
                )

            # If there's a launch error, transition to ALLOCATION_FAILED.
            launch_error = launch_errors.get(im_instance.launch_request_id)
            if launch_error and launch_error.node_type == im_instance.instance_type:
                return IMInstanceUpdateEvent(
                    instance_id=im_instance.instance_id,
                    new_instance_status=IMInstance.ALLOCATION_FAILED,
                    details=launch_error.details,
                )
            # No update.
            return None

        def _retry_or_fail(im_instance) -> Optional[IMInstanceUpdateEvent]:
            # If we have been stuck in this allocated status for too long, we should
            # either retry or fail.
            timeout_s = config.request_status_timeout_s
            max_num_retry_request_to_allocate = config.max_num_retry_request_to_allocate

            all_request_times_ns = sorted(
                InstanceUtil.get_status_transition_times_ns(
                    im_instance, select_instance_status=IMInstance.REQUESTED
                )
            )

            # Fail the allocation if we have tried too many times.
            if len(all_request_times_ns) > max_num_retry_request_to_allocate:
                return IMInstanceUpdateEvent(
                    instance_id=im_instance.instance_id,
                    new_instance_status=IMInstance.ALLOCATION_FAILED,
                    details=(
                        "Failed to allocate cloud instance after "
                        f"{len(all_request_times_ns)} attempts"
                    ),
                )

            # Retry the allocation if we have waited for too long.
            last_request_time_ns = all_request_times_ns[-1]
            if time.time_ns() - last_request_time_ns > timeout_s * 1e9:
                return IMInstanceUpdateEvent(
                    instance_id=im_instance.instance_id,
                    new_instance_status=IMInstance.QUEUED,
                    details=f"Retry allocation after timeout={timeout_s}s",
                )
            return None

        for instance in instances_with_launch_requests:
            # Try allocate or fail with errors.
            update_event = _allocate_or_fail(instance)
            if not update_event:
                update_event = _retry_or_fail(instance)

            if not update_event:
                continue

            logger.debug("Updating {}".format(MessageToDict(update_event)))
            updates[instance.instance_id] = update_event

        # Update the instance manager for the events.
        Reconciler._update_instance_manager(instance_manager, updates, version)

    @staticmethod
    def _handle_ray_running(
        instance_manager: InstanceManager, ray_nodes: List[NodeState]
    ):
        pass

    @staticmethod
    def _handle_ray_stopped(
        instance_manager: InstanceManager, ray_nodes: List[NodeState]
    ):
        pass

    @staticmethod
    def _handle_ray_install_failed(
        instance_manager: InstanceManager, ray_install_errors: List[RayInstallError]
    ):
        pass

    @staticmethod
    def _handle_cloud_instance_terminated(
        instance_manager: InstanceManager,
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
        cloud_provider_errors: List[CloudInstanceProviderError],
    ):
        pass

    @staticmethod
    def _get_im_instances(
        instance_manager: InstanceManager,
    ) -> Tuple[List[IMInstance], int]:
        reply = instance_manager.get_instance_manager_state(
            request=GetInstanceManagerStateRequest()
        )
        assert reply.status.code == StatusCode.OK
        im_state = reply.state
        return im_state.instances, im_state.version

    @staticmethod
    def _update_instance_manager(
        instance_manager: InstanceManager,
        updates: Dict[str, IMInstanceUpdateEvent],
        version: int,
    ) -> None:
        if not updates:
            return
        reply = instance_manager.update_instance_manager_state(
            request=UpdateInstanceManagerStateRequest(
                expected_version=version,
                updates=list(updates.values()),
            )
        )
        assert (
            reply.status.code == StatusCode.OK
        ), f"Failed to update instance manager: {reply}"
