import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.core.generated.instance_manager_pb2 import (
    GetInstanceManagerStateReply,
    GetInstanceManagerStateRequest,
    Instance,
    InstanceUpdateEvent,
    NodeKind,
    StatusCode,
    UpdateInstanceManagerStateReply,
    UpdateInstanceManagerStateRequest,
)

logger = logging.getLogger(__name__)


@dataclass
class _InstanceStatusSummary:
    """Lifetime summaries independent of the bounded diagnostic history."""

    counts: Dict[int, int] = field(default_factory=dict)
    first_times_ns: Dict[int, int] = field(default_factory=dict)

    def record_transition(self, transition: Instance.StatusHistory) -> None:
        status = transition.instance_status
        self.counts[status] = self.counts.get(status, 0) + 1
        self.first_times_ns[status] = min(
            self.first_times_ns.get(status, transition.timestamp_ns),
            transition.timestamp_ns,
        )


class InstanceUpdatedSubscriber(ABC):
    """Subscribers to instance status changes."""

    @abstractmethod
    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        pass


class InstanceManager:
    """
    See `InstanceManagerService` in instance_manager.proto

    This handles updates to an instance, or inserts a new instance if
    it's an insert update. We should only be inserting new instances
    of the below statuses:
        1. ALLOCATED: For unmanaged instance not initialized by InstanceManager,
            e.g. head node
        2. QUEUED: For new instance being queued to launch.
        3. TERMINATING: For leaked cloud instance that needs to be terminated.

    For full status transitions, see:
    https://docs.google.com/document/d/1NzQjA8Mh-oMc-QxXOa529oneWCoA8sDiVoNkBqqDb4U/edit#heading=h.k9a1sp4qpqj4

    Not thread safe, should be used as a singleton.
    """

    def __init__(
        self,
        instance_storage: InstanceStorage,
        instance_status_update_subscribers: Optional[List[InstanceUpdatedSubscriber]],
    ):
        self._instance_storage = instance_storage
        self._status_update_subscribers = instance_status_update_subscribers or []
        # Like the instance storage used by Autoscaler, summaries live only in
        # this process. Each summary has at most one entry per status.
        self._status_summaries: Dict[str, _InstanceStatusSummary] = {}

    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        """
        Updates the instance manager state.

        If there's any failure, no updates would be made and the reply
        would contain the latest version of the instance manager state,
        and the error info.

        Args:
            request: The request to update the instance manager state.

        Returns:
            The reply to the request.
        """

        # Handle updates
        ids_to_updates = {update.instance_id: update for update in request.updates}
        to_update_instances, version = self._instance_storage.get_instances(
            instance_ids=ids_to_updates.keys()
        )

        if request.expected_version >= 0 and request.expected_version != version:
            err_str = (
                f"Version mismatch: expected: {request.expected_version}, "
                f"actual: {version}"
            )
            logger.warning(err_str)
            return self._get_update_im_state_reply(
                StatusCode.VERSION_MISMATCH,
                version,
                err_str,
            )

        # Handle instances states update.
        to_upsert_instances = []
        to_update_summaries = {}
        for instance_id, update in ids_to_updates.items():
            if instance_id in to_update_instances:
                # Seed from stored history before the next transition trims it.
                summary = self._get_status_summary(to_update_instances[instance_id])
                instance = self._update_instance(
                    to_update_instances[instance_id], update
                )
            else:
                summary = _InstanceStatusSummary()
                instance = self._create_instance(update)

            to_upsert_instances.append(instance)
            to_update_summaries[instance_id] = summary

        # Updates the instance storage.
        result = self._instance_storage.batch_upsert_instances(
            updates=to_upsert_instances,
            expected_storage_version=version,
        )

        if not result.success:
            if result.version != version:
                err_str = (
                    f"Version mismatch: expected: {version}, actual: {result.version}"
                )
                logger.warning(err_str)
                return self._get_update_im_state_reply(
                    StatusCode.VERSION_MISMATCH, result.version, err_str
                )
            else:
                err_str = "Failed to update instance storage."
                logger.error(err_str)
                return self._get_update_im_state_reply(
                    StatusCode.UNKNOWN_ERRORS, result.version, err_str
                )

        # Successful updates.
        # Update summaries only after storage commits, avoiding a copy for every
        # transition while leaving them unchanged if the write fails.
        for instance in to_upsert_instances:
            to_update_summaries[instance.instance_id].record_transition(
                instance.status_history[-1]
            )
        self._status_summaries.update(to_update_summaries)
        for subscriber in self._status_update_subscribers:
            subscriber.notify(request.updates)

        return self._get_update_im_state_reply(StatusCode.OK, result.version)

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
        reply = GetInstanceManagerStateReply()
        instances, version = self._instance_storage.get_instances()
        reply.state.instances.extend(instances.values())
        reply.state.version = version
        reply.status.code = StatusCode.OK

        return reply

    def get_status_transition_count(
        self, instance: Instance, status: Instance.InstanceStatus
    ) -> int:
        """Return the lifetime number of transitions into a status.

        Existing instances are initialized from their available history.

        Args:
            instance: The instance to check.
            status: The status whose transitions to count.

        Returns:
            The transition count, or zero if the status is absent.
        """
        return self._get_status_summary(instance).counts.get(status, 0)

    def get_first_status_transition_time_ns(
        self, instance: Instance, status: Instance.InstanceStatus
    ) -> Optional[int]:
        """Return the first transition time into a status.

        Existing instances are initialized from their available history.

        Args:
            instance: The instance to check.
            status: The status whose first transition to find.

        Returns:
            The first timestamp in nanoseconds, or None if the status is absent.
        """
        return self._get_status_summary(instance).first_times_ns.get(status)

    def delete_instances(
        self, instance_ids: List[str], expected_storage_version: Optional[int] = None
    ) -> Tuple[bool, int]:
        """Delete instance records from the storage.

        Used to reclaim terminal (TERMINATED) records that no status transition
        removes. Subscribers are intentionally not notified: these records carry
        no outgoing transition, so there is no InstanceUpdateEvent to emit.

        Args:
            instance_ids: The instance ids to delete.
            expected_storage_version: If set, the delete is applied only when the
                storage is still at this version (optimistic concurrency).

        Returns:
            A (success, version) tuple. success is False on a version mismatch.
        """
        result = self._instance_storage.batch_delete_instances(
            instance_ids=instance_ids,
            expected_storage_version=expected_storage_version,
        )
        if result.success:
            for instance_id in instance_ids:
                self._status_summaries.pop(instance_id, None)
        return result

    #########################################
    # Private methods
    #########################################

    def _get_status_summary(self, instance: Instance) -> _InstanceStatusSummary:
        if instance.instance_id not in self._status_summaries:
            summary = _InstanceStatusSummary()
            for transition in instance.status_history:
                summary.record_transition(transition)
            self._status_summaries[instance.instance_id] = summary
        return self._status_summaries[instance.instance_id]

    @staticmethod
    def _get_update_im_state_reply(
        status_code: StatusCode, version: int, error_message: str = ""
    ) -> UpdateInstanceManagerStateReply:
        """
        Returns a UpdateInstanceManagerStateReply with the given status code and
        version.

        Args:
            status_code: The status code.
            version: The version.
            error_message: The error message if any.

        Returns:
            The reply.
        """
        reply = UpdateInstanceManagerStateReply()
        reply.status.code = status_code
        reply.version = version
        if error_message:
            reply.status.message = error_message
        return reply

    @staticmethod
    def _apply_update(instance: Instance, update: InstanceUpdateEvent):
        """
        Apply status specific update to the instance.

        Args:
            instance: The instance to update.
            update: The update to apply.
        """
        if update.new_instance_status == Instance.ALLOCATED:
            assert (
                update.cloud_instance_id
            ), "ALLOCATED update must have cloud_instance_id"
            assert update.node_kind in [
                NodeKind.WORKER,
                NodeKind.HEAD,
            ], "ALLOCATED update must have node_kind as WORKER or HEAD"
            assert update.instance_type, "ALLOCATED update must have instance_type"
            assert (
                update.cloud_instance_id
            ), "ALLOCATED update must have cloud_instance_id"
            instance.cloud_instance_id = update.cloud_instance_id
            instance.node_kind = update.node_kind
            instance.instance_type = update.instance_type
            instance.node_id = update.ray_node_id
        elif update.new_instance_status == Instance.RAY_RUNNING:
            assert update.ray_node_id, "RAY_RUNNING update must have ray_node_id"
            instance.node_id = update.ray_node_id
        elif update.new_instance_status == Instance.REQUESTED:
            assert (
                update.launch_request_id
            ), "REQUESTED update must have launch_request_id"
            assert update.instance_type, "REQUESTED update must have instance_type"
            instance.launch_request_id = update.launch_request_id
            instance.instance_type = update.instance_type
        elif update.new_instance_status == Instance.TERMINATING:
            assert (
                update.cloud_instance_id
            ), "TERMINATING update must have cloud instance id"

    @staticmethod
    def _create_instance(update: InstanceUpdateEvent) -> Instance:
        """
        Create a new instance from the given update.
        """

        assert update.upsert, "upsert must be true for creating new instance."

        assert update.new_instance_status in [
            # For unmanaged instance not initialized by InstanceManager,
            # e.g. head node
            Instance.ALLOCATED,
            # For new instance being queued to launch.
            Instance.QUEUED,
            # For leaked cloud instance that needs to be terminated.
            Instance.TERMINATING,
        ], (
            "Invalid status for new instance, must be one of "
            "[ALLOCATED, QUEUED, TERMINATING]"
        )

        # Create a new instance first for common fields.
        instance = InstanceUtil.new_instance(
            instance_id=update.instance_id,
            instance_type=update.instance_type,
            status=update.new_instance_status,
            details=update.details,
        )

        # Apply the status specific updates.
        logger.info(InstanceUtil.get_log_str_for_update(instance, update))
        InstanceManager._apply_update(instance, update)
        return instance

    @staticmethod
    def _update_instance(instance: Instance, update: InstanceUpdateEvent) -> Instance:
        """
        Update the instance with the given update.

        Args:
            instance: The instance to update.
            update: The update to apply.

        Returns:
            The updated instance.
        """
        logger.info(InstanceUtil.get_log_str_for_update(instance, update))
        assert InstanceUtil.set_status(instance, update.new_instance_status), (
            "Invalid status transition from "
            f"{Instance.InstanceStatus.Name(instance.status)} to "
            f"{Instance.InstanceStatus.Name(update.new_instance_status)}"
        )
        InstanceManager._apply_update(instance, update)

        return instance
