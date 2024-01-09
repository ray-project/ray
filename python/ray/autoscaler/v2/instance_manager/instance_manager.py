import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set

from ray.autoscaler.v2.schema import InvalidInstanceStatusError
from ray.core.generated.instance_manager_pb2 import (
    GetInstanceManagerStateReply,
    GetInstanceManagerStateRequest,
    Instance,
    UpdateInstanceManagerStateReply,
    UpdateInstanceManagerStateRequest,
)

logger = logging.getLogger(__name__)


class InstanceManager(ABC):
    """
    See `InstanceManagerService` in instance_manager.proto

    This handles the following updates to an instance:
        1. when creating new instances
            An instance is created from an autoscaler's launch request.
            This initializes an instance object with:
            status = Instance.QUEUED
        2. when ray is stopping
            This happens when the autoscaler is terminating the ray
            process on the instance, e.g. idle termination.
            status = Instance.RAY_STOPPING
        3. when ray is already stopped.
            Only the ray cluster has the true status of the ray process
            on an instance, so autoscaler will update an instance's ray
            to be stopped.
            status = Instance.RAY_STOPPED

    For full status transitions, see:
    https://docs.google.com/document/d/1NzQjA8Mh-oMc-QxXOa529oneWCoA8sDiVoNkBqqDb4U/edit#heading=h.k9a1sp4qpqj4
    """

    @abstractmethod
    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        """
        Updates the instance manager state.

        Args:
            request: The request to update the instance manager state.

        Returns:
            The reply to the request.
        """
        pass

    @abstractmethod
    def get_instance_manager_state(
        self, request: GetInstanceManagerStateRequest
    ) -> GetInstanceManagerStateReply:
        """
        Gets the instance manager state.

        Args:
            request: The request to get the instance manager state.

        Returns:
            The reply to the request.
        """
        pass


class InstanceUtil:
    """
    A helper class to group updates and operations on an Instance object defined
    in instance_manager.proto
    """

    @staticmethod
    def new_instance(
        instance_id: str,
        instance_type: str,
        request_id: str = "",
    ) -> Instance:
        instance = Instance()
        instance.version = 0  # it will be populated by the underlying storage.
        instance.instance_id = instance_id
        instance.instance_type = instance_type
        instance.launch_request_id = request_id
        instance.status = Instance.QUEUED
        InstanceUtil._record_status_transition(
            instance, Instance.QUEUED, "created from InstanceUtil"
        )
        return instance

    @staticmethod
    def is_cloud_instance_allocated(instance: Instance) -> bool:
        """
        Returns True if the instance is in a status where there could exist
        a cloud instance allocated by the cloud provider.
        """
        assert instance.status != Instance.UNKNOWN
        return instance.status in {
            Instance.ALLOCATED,
            Instance.RAY_INSTALLING,
            Instance.RAY_RUNNING,
            Instance.RAY_STOPPING,
            Instance.RAY_STOPPED,
            Instance.STOPPING,
            Instance.RAY_INSTALL_FAILED,
        }

    @staticmethod
    def is_ray_running_reachable(instance: Instance) -> bool:
        """
        Returns True if the instance is in a status where it may transition
        to RAY_RUNNING status.
        """
        assert instance.status != Instance.UNKNOWN
        return instance.status in [
            Instance.UNKNOWN,
            Instance.QUEUED,
            Instance.REQUESTED,
            Instance.ALLOCATED,
            Instance.RAY_INSTALLING,
            Instance.ALLOCATION_FAILED,
        ]

    @staticmethod
    def set_status(
        instance: Instance,
        new_instance_status: Instance.InstanceStatus,
        details: str = "",
    ):
        """Transitions the instance to the new state.

        Args:
            instance: The instance to update.
            new_instance_status: The new status to transition to.
            details: The details of the transition.

        Raises:
            InvalidInstanceStatusError if the transition is not allowed.
        """
        if (
            new_instance_status
            not in InstanceUtil.get_valid_transitions()[instance.status]
        ):
            raise InvalidInstanceStatusError(
                instance_id=instance.instance_id,
                cur_status=instance.status,
                new_status=new_instance_status,
            )
        instance.status = new_instance_status
        InstanceUtil._record_status_transition(instance, new_instance_status, details)

    @staticmethod
    def _record_status_transition(
        instance: Instance, status: Instance.InstanceStatus, details: str
    ):
        """Records the status transition.

        Args:
            instance: The instance to update.
            status: The new status to transition to.
        """
        now_ns = time.time_ns()
        instance.status_history.append(
            Instance.StatusHistory(
                instance_status=status,
                timestamp_ns=now_ns,
                details=details,
            )
        )

    @staticmethod
    def get_valid_transitions() -> Dict[
        "Instance.InstanceStatus", Set["Instance.InstanceStatus"]
    ]:
        return {
            # This is the initial status of a new instance.
            Instance.QUEUED: {
                # Cloud provider requested to launch a node for the instance.
                # This happens when the a launch request is made to the node provider.
                Instance.REQUESTED
            },
            # When in this status, a launch request to the node provider is made.
            Instance.REQUESTED: {
                # Cloud provider allocated a cloud node for the instance.
                # This happens when the cloud node first appears in the list of running
                # cloud nodes from the cloud node provider.
                Instance.ALLOCATED,
                # Cloud provider fails to allocate one. Either as a timeout or
                # the launch request fails immediately.
                Instance.ALLOCATION_FAILED,
            },
            # When in this status, the cloud node is allocated and running. This
            # happens when the cloud node is present in node provider's list of
            # running cloud nodes.
            Instance.ALLOCATED: {
                # Ray needs to be install and launch on the provisioned cloud node.
                # This happens when the cloud node is allocated, and the autoscaler
                # is responsible for installing and launching ray on the cloud node.
                # For node provider that manages the ray installation and launching,
                # this state is skipped.
                Instance.RAY_INSTALLING,
                # Ray is already installed and running on the provisioned cloud node.
                # This happens when a ray node joins the ray cluster, and the instance
                # is discovered in the set of running ray nodes from the Ray cluster.
                Instance.RAY_RUNNING,
                # Instance is requested to be stopped, e.g. instance leaked: no matching
                # Instance with the same type is found in the autoscaler's state.
                Instance.STOPPING,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            # Ray process is being installed and started on the cloud node.
            # This status is skipped for node provider that manages the ray
            # installation and launching. (e.g. Ray-on-Spark)
            Instance.RAY_INSTALLING: {
                # Ray installed and launched successfully, reported by the ray cluster.
                # Similar to the Instance.ALLOCATED -> Instance.RAY_RUNNING transition,
                # where the ray process is managed by the node provider.
                Instance.RAY_RUNNING,
                # Ray installation failed. This happens when the ray process failed to
                # be installed and started on the cloud node.
                Instance.RAY_INSTALL_FAILED,
                # Wen the ray node is reported as stopped by the ray cluster.
                # This could happen that the ray process was stopped quickly after start
                # such that a ray running node  wasn't discovered and the RAY_RUNNING
                # transition was skipped.
                Instance.RAY_STOPPED,
                # Cloud node somehow failed during the installation process.
                Instance.STOPPED,
            },
            # Ray process is installed and running on the cloud node. When in this
            # status, a ray node must be present in the ray cluster.
            Instance.RAY_RUNNING: {
                # Ray is requested to be stopped to the ray cluster,
                # e.g. idle termination.
                Instance.RAY_STOPPING,
                # Ray is already stopped, as reported by the ray cluster.
                Instance.RAY_STOPPED,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            # When in this status, the ray process is requested to be stopped to the
            # ray cluster, but not yet present in the dead ray node list reported by
            # the ray cluster.
            Instance.RAY_STOPPING: {
                # Ray is stopped, and the ray node is present in the dead ray node list
                # reported by the ray cluster.
                Instance.RAY_STOPPED,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            # When in this status, the ray process is stopped, and the ray node is
            # present in the dead ray node list reported by the ray cluster.
            Instance.RAY_STOPPED: {
                # Cloud node is requested to be stopped.
                Instance.STOPPING,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            # When in this status, the cloud node is requested to be stopped to
            # the node provider.
            Instance.STOPPING: {
                # When a cloud node no longer appears in the list of running cloud nodes
                # from the node provider.
                Instance.STOPPED
            },
            # Whenever a cloud node disappears from the list of running cloud nodes
            # from the node provider, the instance is marked as stopped. Since
            # we guarantee 1:1 mapping of a Instance to a cloud node, this is a
            # terminal state.
            Instance.STOPPED: set(),  # Terminal state.
            # When in this status, the cloud node failed to be allocated by the
            # node provider.
            Instance.ALLOCATION_FAILED: {
                # Autoscaler might retry to allocate the instance.
                Instance.QUEUED
            },
            Instance.RAY_INSTALL_FAILED: {
                # Autoscaler requests to shutdown the instance when ray install failed.
                Instance.STOPPING,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            # Initial state before the instance is created. Should never be used.
            Instance.UNKNOWN: set(),
        }

    @staticmethod
    def get_status_times_ns(
        instance: Instance,
        select_instance_status: Optional["Instance.InstanceStatus"] = None,
    ) -> List[int]:
        """
        Returns a list of timestamps of the instance status update.

        Args:
            instance: The instance.
            instance_status: The status to search for. If None, returns all
                status updates timestamps.

        Returns:
            The list of timestamps of the instance status updates.
        """
        ts_list = []
        for status_update in instance.status_history:
            if (
                select_instance_status
                and status_update.instance_status != select_instance_status
            ):
                continue
            ts_list.append(status_update.timestamp_ns)

        return ts_list
