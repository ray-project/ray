from dataclasses import dataclass
import logging
from collections import defaultdict
import time
from typing import Dict, List, Optional, Set

from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    CloudInstanceId,
    CloudInstanceProviderError,
    LaunchNodeError,
    TerminateNodeError,
)
from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.core.generated.autoscaler_pb2 import ClusterResourceState, NodeState
from ray.core.generated.instance_manager_pb2 import Instance as IMInstance
from ray.core.generated.instance_manager_pb2 import (
    InstanceUpdateEvent as IMInstanceUpdateEvent,
)

logger = logging.getLogger(__name__)


class Reconciler:
    @staticmethod
    def reconcile(
        im_instances: List[IMInstance],
        ray_cluster_resource_state: ClusterResourceState,
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
        cloud_provider_errors: List[CloudInstanceProviderError],
    ) -> Dict[str, IMInstanceUpdateEvent]:
        """
        Reconcile the instance states of the instance manager with the cloud provider's
        states and the ray cluster's states.

        For each instance, we try to figure out if we need to transition the instance
        status to a new status, and if so, what the new status should be.

        These events are then consumed by the InstanceManager's subscribers for actions.

        Args:
            im_instances: The instance manager's instances.
            ray_cluster_resource_state: The ray cluster's resource state.
            non_terminated_cloud_instances: The non-terminated cloud instances.
            cloud_provider_errors: The cloud provider errors.

        Returns:
            A dictionary of instance id to instance update event.

        """
        updates = {}
        ctx = Reconciler.ReconcileContext(
            ray_cluster_resource_state=ray_cluster_resource_state,
            non_terminated_cloud_instances=non_terminated_cloud_instances,
            cloud_provider_errors=cloud_provider_errors,
            im_instances=im_instances,
        )

        for im_instance in im_instances:
            if im_instance.status == IMInstance.QUEUED:
                raise NotImplementedError("Not implemented")
            elif im_instance.status == IMInstance.REQUESTED:
                handle_fn = Reconciler._handle_requested_instance
            elif im_instance.status == IMInstance.ALLOCATED:
                raise NotImplementedError("Not implemented")
            elif im_instance.status == IMInstance.RAY_INSTALLING:
                raise NotImplementedError("Not implemented")
            elif im_instance.status == IMInstance.RAY_RUNNING:
                raise NotImplementedError("Not implemented")
            elif im_instance.status == IMInstance.RAY_STOPPING:
                raise NotImplementedError("Not implemented")
            elif im_instance.status == IMInstance.RAY_STOPPED:
                raise NotImplementedError("Not implemented")
            elif im_instance.status == IMInstance.TERMINATING:
                raise NotImplementedError("Not implemented")
            elif im_instance.status == IMInstance.TERMINATED:
                raise NotImplementedError("Not implemented")
            elif im_instance.status == IMInstance.ALLOCATION_FAILED:
                raise NotImplementedError("Not implemented")
            elif im_instance.status == IMInstance.RAY_INSTALL_FAILED:
                raise NotImplementedError("Not implemented")
            else:
                raise ValueError(f"Unknown instance status: {im_instance.status}")

            update = handle_fn(im_instance, ctx)

            if update:
                updates[im_instance.instance_id] = update

        return updates

    @staticmethod
    def _handle_requested_instance(
        im_instance: IMInstance,
        ctx: "Reconciler.ReconcileContext",
    ) -> Optional[IMInstanceUpdateEvent]:
        def _allocate_or_fail(
            im_instance: IMInstance, ctx: "Reconciler.ReconcileContext"
        ) -> Optional[IMInstanceUpdateEvent]:
            # If there's an unassigned cloud instance with the same request id, transition to ALLOCATED.
            assert (
                im_instance.launch_request_id
            ), f"Requested instance({im_instance.instance_id}) has no launch request id."

            unassigned_cloud_instance = ctx.take_cloud_instance(
                request_id=im_instance.launch_request_id,
                unassigned=True,
                instance_type=im_instance.instance_type,
            )

            if unassigned_cloud_instance:
                return IMInstanceUpdateEvent(
                    instance_id=im_instance.instance_id,
                    new_instance_status=IMInstance.ALLOCATED,
                    cloud_instance_id=unassigned_cloud_instance.cloud_instance_id,
                )

            # If there's a launch error, transition to ALLOCATION_FAILED.
            launch_error = ctx.get_launch_error(
                im_instance.launch_request_id, instance_type=im_instance.instance_type
            )
            if launch_error:
                return IMInstanceUpdateEvent(
                    instance_id=im_instance.instance_id,
                    new_instance_status=IMInstance.ALLOCATION_FAILED,
                    error_message=launch_error.details,
                )
            return None

        def _retry_or_fail(im_instance, ctx) -> Optional[IMInstanceUpdateEvent]:
            # If we have been stuck in this allocated status for too long, we should
            # either retry or fail.
            timeout_s = ctx.get_allocate_status_timeout_s()
            max_num_request_to_allocate = ctx.get_max_num_request_to_allocate()

            all_request_times_ns = sorted(
                InstanceUtil.get_status_transition_times_ns(
                    im_instance, select_instance_status=IMInstance.REQUESTED
                )
            )

            # Fail the allocation if we have tried too many times.
            if len(all_request_times_ns) >= max_num_request_to_allocate:
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

        # Try to allocate from an unassigned cloud instance or fail with a launch error.
        update = _allocate_or_fail(im_instance, ctx)
        if update:
            return update

        # If we have been stuck in this allocated status for too long, we should
        # either retry or fail.
        update = _retry_or_fail(im_instance, ctx)
        if update:
            return update

        return None

    @dataclass
    class ReconcileContext:
        """A utility class of context for reconciler

        This class encapsulates the data that's used for reconciling the instance
        manager's state with the cloud provider's state, and the ray cluster's state.
        """

        # Ray nodes
        ray_nodes: Dict[CloudInstanceId, NodeState]

        # Cloud provider errors
        launch_errors: Dict[str, LaunchNodeError]
        terminate_errors: Dict[CloudInstanceId, TerminateNodeError]

        # Primary index for the cloud instances.
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance]
        # Secondary indexes for the cloud instances.
        cloud_instances_if_assigned: Dict[bool, Set[CloudInstanceId]]
        cloud_instances_by_request_id: Dict[str, Set[CloudInstanceId]]
        cloud_instances_by_instance_type: Dict[str, Set[CloudInstanceId]]

        def __init__(
            self,
            ray_cluster_resource_state: ClusterResourceState,
            non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
            cloud_provider_errors: List[CloudInstanceProviderError],
            im_instances: List[IMInstance],
        ):
            # Split the errors
            for error in cloud_provider_errors:
                if isinstance(error, LaunchNodeError):
                    self.launch_errors[error.request_id] = error
                elif isinstance(error, TerminateNodeError):
                    self.terminate_errors[error.cloud_instance_id] = error
                else:
                    raise ValueError(f"Unknown cloud provider error: {error}")

            # Group the ray nodes by cloud instance id.
            for node in ray_cluster_resource_state.nodes_states:
                if not node.instance_id:
                    logger.warning(
                        f"Ray ode {node.node_id} has no instance_id. Skipping reconcile."
                        "This is likely a ray node that's not managed by ray autoscaler, "
                        "or the cloud provider failed to inject the cloud instance id "
                        "into the ray node."
                    )
                    continue
                self.ray_nodes[node.instance_id] = node

            # Initialize the states for cloud instances.
            self.non_terminated_cloud_instances = non_terminated_cloud_instances
            self.cloud_instances_if_assigned = defaultdict(set)
            self.cloud_instances_by_request_id = defaultdict(set)
            self.cloud_instances_by_instance_type = defaultdict(set)

            im_instances_with_cloud_instance_id = {
                im_instance.cloud_instance_id: im_instance
                for im_instance in im_instances
                if im_instance.cloud_instance_id
            }

            for (
                cloud_instance_id,
                cloud_instance,
            ) in non_terminated_cloud_instances.items():
                if cloud_instance_id in im_instances_with_cloud_instance_id:
                    self.cloud_instances_if_assigned[True].add(cloud_instance_id)
                else:
                    self.cloud_instances_if_assigned[False].add(cloud_instance_id)

                self.cloud_instances_by_request_id[cloud_instance.request_id].add(
                    cloud_instance_id
                )
                self.cloud_instances_by_instance_type[cloud_instance.node_type].add(
                    cloud_instance_id
                )

        def take_cloud_instance(
            self, request_id: str, unassigned: bool, instance_type: str
        ) -> Optional[CloudInstance]:
            """
            Take an unassigned cloud instance that matches the request_id and
            instance_type from the unassigned cloud instances.
            """
            by_request = self.cloud_instances_by_request_id.get(request_id, set())
            by_type = self.cloud_instances_by_instance_type.get(instance_type, set())
            by_assignment = self.cloud_instances_by_instance_type.get(unassigned, set())

            matched = by_request & by_type & by_assignment
            if not matched:
                return None
            matched_list = list(matched)
            unassigned_cloud_instance_id = matched_list.pop()

            # Update the secondary indexes.
            self.cloud_instances_by_request_id[request_id].remove(
                unassigned_cloud_instance_id
            )
            self.cloud_instances_by_instance_type[instance_type].remove(
                unassigned_cloud_instance_id
            )
            self.cloud_instances_by_instance_type[unassigned].remove(
                unassigned_cloud_instance_id
            )

            # Update the primary index and get the cloud instance.
            unassigned_cloud_instance = self.non_terminated_cloud_instances.pop(
                unassigned_cloud_instance_id
            )
            return unassigned_cloud_instance
