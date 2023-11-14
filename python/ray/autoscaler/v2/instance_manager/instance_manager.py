import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, Optional

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
            status = (Instance.UNKNOWN, Instance.RAY_STATUS_UNKNOWN)
        2. when ray is stopping
            This happens when the autoscaler is terminating the ray
            process on the instance, e.g. idle termination.
            status = (Instance.ALLOCATED, Instance.RAY_STOPPING)
        3. when ray is already stopped.
            Only the ray cluster has the true status of the ray process
            on an instance, so autoscaler will update an instance's ray
            to be stopped.
            status = (Instance.ALLOCATED, Instance.RAY_STOPPED)

    For full status transitions, see:
    https://docs.google.com/document/d/1NzQjA8Mh-oMc-QxXOa529oneWCoA8sDiVoNkBqqDb4U/edit#heading=h.k9a1sp4qpqj4
    """

    @abstractmethod
    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        pass

    @abstractmethod
    def get_instance_manager_state(
        self, request: GetInstanceManagerStateRequest
    ) -> GetInstanceManagerStateReply:
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
        resources: Dict[str, float],
        request_id: str = "",
    ) -> Instance:
        instance = Instance()
        instance.version = 0  # it will be populated by the underlying storage.
        instance.instance_id = instance_id
        instance.instance_type = instance_type
        instance.launch_request_id = request_id
        instance.total_resources.update(resources)
        InstanceUtil.set_status(instance, Instance.UNKNOWN, Instance.RAY_STATUS_UNKNOWN)
        return instance

    @staticmethod
    def is_allocated(instance: Instance) -> bool:
        return instance.ray_status == Instance.ALLOCATED

    @staticmethod
    def is_pending(instance: Instance) -> bool:
        return instance.ray_status in [
            Instance.REQUESTED,
            Instance.QUEUED,
            Instance.UNKNOWN,
        ]

    @staticmethod
    def set_status(
        instance: Instance,
        new_instance_status: Optional["Instance.Status"] = None,
        new_ray_status: Optional["Instance.RayStatus"] = None,
    ):
        """Transitions the instance to the new state.

        This should be called by the reconciler and the instance manager.
        """
        if new_instance_status is not None:
            instance.status = new_instance_status

        if new_ray_status is not None:
            instance.ray_status = new_ray_status

        InstanceUtil.check_valid_instance(instance)

        now_ms = time.time_ns() // 1000000
        if len(instance.status_history) > 0:
            assert (
                instance.status_history[-1].timestamp_ms <= now_ms
            ), "Status history is not sorted"

        instance.status_history.append(
            Instance.StatusHistory(
                instance_status=instance.status,
                ray_status=instance.ray_status,
                timestamp_ms=now_ms,
            )
        )

    @staticmethod
    def get_status_time_s(
        instance: Instance,
        instance_status: Optional["Instance.InstanceStatus"] = None,
        ray_status: Optional["Instance.RayStatus"] = None,
        reverse: bool = False,
    ) -> Optional[int]:
        assert instance_status or ray_status, "Must specify at least one status"

        for status_update in sorted(
            instance.status_history, key=lambda x: x.timestamp_ms, reverse=reverse
        ):
            if instance_status and status_update.instance_status != instance_status:
                continue

            if ray_status and status_update.ray_status != ray_status:
                continue

            return status_update.timestamp_ms // 1000

        return None

    @staticmethod
    def check_valid_instance(instance: Instance):
        """Returns true if the instance is valid."""

        # Check on the instance status and ray status combo.
        if instance.status in [
            Instance.UNKNOWN,
            Instance.QUEUED,
            Instance.REQUESTED,
            Instance.STOPPING,
            Instance.STOPPED,
            Instance.GARBAGE_COLLECTED,
        ]:
            assert instance.ray_status == Instance.RAY_STATUS_UNKNOWN

        if instance.status == Instance.ALLOCATED:
            # This is technically all ray state. Just for clarity.
            assert instance.ray_status in [
                Instance.RAY_STATUS_UNKNOWN,
                Instance.RAY_INSTALLING,
                Instance.RAY_RUNNING,
                Instance.RAY_STOPPING,
                Instance.RAY_STOPPED,
                Instance.RAY_INSTALL_FAILED,
            ]
