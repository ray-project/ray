import logging
import uuid

from ray.autoscaler.v2.instance_manager.common import (
    InstanceUtil,
    InvalidInstanceStatusTransitionError,
)
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.core.generated.instance_manager_pb2 import (
    GetInstanceManagerStateReply,
    GetInstanceManagerStateRequest,
    StatusCode,
    UpdateInstanceManagerStateReply,
    UpdateInstanceManagerStateRequest,
)

logger = logging.getLogger(__name__)


class InstanceManager:
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

    Not thread safe, should be used as a singleton.
    """

    def __init__(self, instance_storage: InstanceStorage):
        self._instance_storage = instance_storage

    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        """
        Updates the instance manager state.

        If there's a any failure, no updates would be made and the reply
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
            ids_to_updates.keys() if ids_to_updates else {}
        )

        if request.expected_version >= 0 and request.expected_version != version:
            err_str = (
                f"Version mismatch: expected: {request.expected_version}, "
                f"actual: {version}"
            )
            logger.warn(err_str)
            return self._get_update_im_state_reply(
                StatusCode.VERSION_MISMATCH,
                version,
                err_str,
            )

        # Handle instances states update.
        for instance in to_update_instances.values():
            update = ids_to_updates[instance.instance_id]
            try:
                InstanceUtil.set_status(
                    instance, update.new_instance_status, update.details
                )
            except InvalidInstanceStatusTransitionError as e:
                logger.error(e)
                return self._get_update_im_state_reply(
                    StatusCode.INVALID_VALUE, version, str(e)
                )

        # Handle launch requests.
        new_instances = []
        for request in request.launch_requests:
            for _ in range(request.count):
                instance = InstanceUtil.new_instance(
                    instance_id=self._random_instance_id(),
                    instance_type=request.instance_type,
                    request_id=request.id,
                )
                new_instances.append(instance)

        # Updates the instance storage.
        result = self._instance_storage.batch_upsert_instances(
            updates=list(to_update_instances.values()) + new_instances,
            expected_storage_version=version,
        )

        if not result.success:
            if result.version != version:
                err_str = (
                    f"Version mismatch: expected: {version}, actual: {result.version}"
                )
                logger.warn(err_str)
                return self._get_update_im_state_reply(
                    StatusCode.VERSION_MISMATCH, result.version, err_str
                )
            else:
                err_str = "Failed to update instance storage."
                logger.error(err_str)
                return self._get_update_im_state_reply(
                    StatusCode.UNKNOWN_ERRORS, result.version, err_str
                )

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

    #########################################
    # Private methods
    #########################################

    def _random_instance_id(self) -> str:
        """
        Returns an instance_id.
        """
        return str(uuid.uuid4())

    def _get_update_im_state_reply(
        self, status_code: StatusCode, version: int, error_message: str = ""
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
