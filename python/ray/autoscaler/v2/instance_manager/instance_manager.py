from collections import defaultdict
import copy
import logging
import time
import uuid
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudProviderNode,
)

from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSubscriber,
)
from ray.core.generated.instance_manager_pb2 import (
    GetAvailableInstanceTypesResponse,
    GetInstanceManagerStateReply,
    Instance,
    InstanceManagerState,
    UpdateInstanceManagerStateReply,
    UpdateInstanceManagerStateRequest,
    LaunchRequest,
)

from ray.core.generated.autoscaler_pb2 import (
    PendingInstanceRequest,
    PendingInstance,
    FailedInstanceRequest,
)

logger = logging.getLogger(__name__)


class InstanceManager(metaclass=ABCMeta):
    @abstractmethod
    def get_available_instance_types(self) -> GetAvailableInstanceTypesResponse:
        pass

    @abstractmethod
    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        pass

    @abstractmethod
    def get_instance_manager_state(self) -> GetInstanceManagerStateReply:
        pass


class SimpleInstanceManager(InstanceManager):
    """Simple instance manager that manages instances in memory.

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

        The instance manager should not update the instance status directly,
        but instead, it should update the instance's ray status. And
        the reconciler/instance launcher will update the instance status
        based on the ray status and the instance status with the underlying
        node provider.

    TODO: status transitions docs
    """

    def __init__(
        self,
        instance_storage: InstanceStorage,
        available_node_types: Dict[str, Any],  # TODO: abstract this
        stopped_node_gc_timeout_s: int = 1800,
        status_change_subscribers: Optional[List[InstanceUpdatedSubscriber]] = None,
    ) -> None:
        super().__init__()
        self._instance_configs = available_node_types
        self._stopped_node_gc_timeout_s = stopped_node_gc_timeout_s
        self._instance_storage = instance_storage
        # An in-memory cache of instance launch requests.
        # This could also go into the storage?
        self._launch_requests: Dict[str, LaunchRequest] = {}
        instance_storage.add_status_change_subscribers(status_change_subscribers)

    def get_available_instance_types(self) -> GetAvailableInstanceTypesResponse:
        return GetAvailableInstanceTypesResponse(instance_types=self._instance_types)

    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        ids_to_updates = {update.instance_id: update for update in request.updates}
        if len(ids_to_updates) == 0:
            # TODO: this special case is kind of weird since the `get_instances`
            # call would return all if empty sets are passed.
            to_update_instances, version = {}, request.expected_version
        else:
            to_update_instances, version = self._instance_storage.get_instances(
                ids_to_updates.keys()
            )
        # handle version mismatch
        if request.expected_version >= 0 and request.expected_version != version:
            reply = UpdateInstanceManagerStateReply()
            reply.success = False
            reply.version = version
            return reply

        # handle instances states update.
        for instance in to_update_instances.values():
            update = ids_to_updates[instance.instance_id]
            # We will not update the underlying instance status since
            # it is managed by the reconciler directly.
            self._transition_ray_status(instance, update.new_ray_status)

        # handle new instances to start
        new_instances = []
        for launch_request in request.launch_requests:
            instances = self._create_instances(launch_request)
            new_instances.extend(instances)
            self._launch_requests[launch_request.id] = launch_request

        expected_version = (
            request.expected_version if request.expected_version else None
        )
        result, version = self._instance_storage.batch_upsert_instances(
            new_instances + list(to_update_instances.values()),
            expected_version,
        )

        reply = UpdateInstanceManagerStateReply()
        reply.success = result
        reply.state.CopyFrom(self.get_instance_manager_state().state)

        # TODO: do we need this version?
        reply.version = reply.state.version
        # assert version == reply.state.version, (
        #     f"version mismatch: {version} vs {reply.state.version}: result {result}"
        # )
        return reply

    def _get_instance_manager_state(self) -> InstanceManagerState:
        instances, version = self._instance_storage.get_instances()
        state = InstanceManagerState()
        state.version = version
        state.instances.extend(instances.values())
        state.launch_requests.extend(self._launch_requests.values())
        return state

    def get_instance_manager_state(self) -> GetInstanceManagerStateReply:
        reply = GetInstanceManagerStateReply()
        reply.state.CopyFrom(self._get_instance_manager_state())
        return reply

    def _transition_ray_status(
        self, instance: Instance, new_ray_status: Instance.RayStatus
    ) -> None:
        # TODO: add status transitions verification.
        assert instance.ray_status != new_ray_status
        InstanceUtil.set_status(instance, new_ray_status=new_ray_status)

    def gc_stopped_nodes(self) -> bool:
        # TODO: should it be part of the reconciler?
        instances, version = self._instance_storage.get_instances()
        to_gc_instances = []
        to_gc_instance_ids = []
        for instance in instances:
            if instance.status == Instance.STOPPED:
                stopped_time_s = get_status_time_s(
                    instance, Instance.STOPPED, Instance.RAY_STATUS_UNKNOWN
                )
                assert stopped_time_s is not None
                if (stopped_time_s + self._stopped_node_gc_timeout_s) > time.time():
                    continue

                logger.info("GCing stopped node %s", instance.instance_id)
                to_gc_instance_ids.append(instance.instance_id)
                to_gc_instances.append(instance)

        if not to_gc_instances:
            return False

        result = self._instance_storage.delete_instances(to_gc_instances, version)[0]
        return result

    def _create_instances(self, request: LaunchRequest) -> List[Instance]:
        instances = []
        instance_type = request.instance_type
        assert instance_type in self._instance_configs, (
            f"instance type {instance_type} not found in "
            f"instance configs {self._instance_configs}"
        )
        for _ in range(request.count):
            instance = self.new_instance(
                instance_id=str(uuid.uuid4()),
                instance_type=instance_type,
                # TODO: do we always have the "resources" field?
                resources=copy.deepcopy(
                    self._instance_configs[instance_type]["resources"]
                ),
                request_id=request.id,
            )

            instances.append(instance)
        return instances

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


class InstanceUtil:
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

        check_valid_instance(instance)

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


def get_pending_instance_requests(
    im_state: InstanceManagerState,
) -> List[PendingInstanceRequest]:
    """Returns a list of instances that are being launched."""
    launch_requests = {r.id: r for r in im_state.launch_requests}

    pending_instances = [
        instance
        for instance in im_state.instances
        if instance.status
        in [
            Instance.REQUESTED,
            Instance.QUEUED,
            Instance.UNKNOWN,
        ]
    ]

    # Group the pending instances by their original launch request.
    instances_by_launch_request = defaultdict(list)
    for instance in pending_instances:
        assert instance.launch_request_id is not None
        instances_by_launch_request[instance.launch_request_id].append(instance)

    results: PendingInstanceRequest = []
    for launch_request_id, instances in instances_by_launch_request.items():
        request = launch_requests[launch_request_id]
        results.append(
            PendingInstanceRequest(
                ray_node_type_name=request.instance_type,
                # TODO(rickyx): need this?
                instance_type_name="",
                count=len(instances),
                request_ts=request.request_ts,
            )
        )

    return results


def get_pending_ray_nodes(im_state: InstanceManagerState) -> List[PendingInstance]:
    """Returns a list of instances that are pending."""
    return [
        PendingInstance(
            ray_node_type_name=instance.instance_type,
            instance_id=instance.instance_id,
            # NOTE: we also have internal ip, but probably not useful.
            ip_address=instance.external_ip,
            # TODO: make below correct
            details="installing ray"
            if instance.ray_status == Instance.RAY_INSTALLING
            else "to install ray",
            instance_type_name="",
        )
        for instance in im_state.instances
        if instance.status in [Instance.ALLOCATED]
        and instance.ray_status
        in [Instance.RAY_INSTALLING, Instance.RAY_STATUS_UNKNOWN]
    ]


def get_failed_instance_requests(
    im_state: InstanceManagerState,
) -> List[FailedInstanceRequest]:
    """Returns a list of instances requests that are failed."""

    return [
        FailedInstanceRequest(
            ray_node_type_name=instance.instance_type,
            instance_type_name="",
            count=1,
            # TODO: we could do better.
            reason=instance.exit_details,
            start_ts=get_status_time_s(
                instance, Instance.UNKNOWN, Instance.RAY_STATUS_UNKNOWN
            )
            or 0,
            failed_ts=get_status_time_s(instance, Instance.ALLOCATION_FAILED, None)
            or 0,
        )
        for instance in im_state.instances
        if instance.status in [Instance.ALLOCATION_FAILED]
    ]


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


def assign_instance_to_cloud_node(
    instance: Instance, cloud_instance: CloudProviderNode
):
    instance.cloud_instance_id = cloud_instance.cloud_instance_id
    instance.internal_ip = cloud_instance.internal_ip
    instance.external_ip = cloud_instance.external_ip
    InstanceUtil.set_status(instance, Instance.ALLOCATED, Instance.RAY_STATUS_UNKNOWN)
