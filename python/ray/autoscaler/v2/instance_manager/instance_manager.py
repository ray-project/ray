import logging
import time
import uuid
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from ray.autoscaler.v2.instance_manager.storage import Storage
from ray.core.generated.instance_manager_pb2 import (
    AvailableInstanceTypes,
    GetAvailableInstanceTypesResponse,
    GetInstanceManagerStateReply,
    Instance,
    InstanceManagerState,
    UpdateInstanceManagerStateReply,
    UpdateInstanceManagerStateRequest,
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


@dataclass
class InstanceStatusChange:
    """Class for keeping track of an item in inventory."""

    instance: Instance
    from_status: int
    to_status: int


class BaseInstanceManager(InstanceManager):
    """BaseInstanceManager is the base class for all instance managers.
    It only manipulates the state of the instances, and does not actually
    calling node provider to create/terminate instances.
    """

    def __init__(
        self,
        cluster_id: str,
        storage: Storage,
        instance_types: AvailableInstanceTypes,
        stopped_node_gc_timeout_s: int = 1800,
    ) -> None:
        super().__init__()
        self._storage = storage
        self._cluster_id = cluster_id
        self._table_name = f"instance_table@{cluster_id}"
        self._instance_types = instance_types
        self._stopped_node_gc_timeout_s = stopped_node_gc_timeout_s

    def get_available_instance_types(self) -> GetAvailableInstanceTypesResponse:
        return GetAvailableInstanceTypesResponse(instance_types=self._instance_types)

    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        mutations = {}

        nodes_to_terminate = list(request.nodes_to_terminate)
        to_terminate_instances, version = self._get_instances(nodes_to_terminate)

        # handle version mismatch
        if request.expected_version >= 0 and request.expected_version != version:
            reply = UpdateInstanceManagerStateReply()
            reply.success = False
            reply.version = version
            return reply

        # handle teriminating instances
        for instance in to_terminate_instances.values():
            if not self._transition_state(instance, Instance.STOPPING):
                reply = UpdateInstanceManagerStateReply()
                reply.success = False
                reply.version = version
                reply.error_message = (
                    f"Failed to transition instance "
                    "{instance.instance_id} from {instance.status} to STOPPING"
                )
                return reply
            mutations[instance.instance_id] = instance.SerializeToString()

        # handle new instances to start
        new_instances = {}
        for instance_type in request.new_nodes_to_start:
            instance = Instance()
            instance.instance_id = str(uuid.uuid4())
            instance.instance_type = instance_type.type_name
            instance.status = Instance.INSTANCE_STATUS_UNSPECIFIED
            new_instances.append(instance)
            mutations[instance.instance_id] = instance.SerializeToString()

        expected_version = (
            request.expected_version if request.expected_version else None
        )
        result, version = self._storage.update(
            self._table_name, mutations, {}, expected_version
        )

        if result:
            if new_instances:
                self._notify_instances_status_changed(
                    new_instances,
                    [None] * len(new_instances),
                    [Instance.INSTANCE_STATUS_UNSPECIFIED] * len(new_instances),
                )
            if to_terminate_instances:
                to_terminate_instances_values = list(to_terminate_instances.values())
                self._notify_instances_status_changed(
                    to_terminate_instances_values,
                    [instance.status for instance in to_terminate_instances_values],
                    [Instance.STOPPING] * len(to_terminate_instances),
                )

        reply = UpdateInstanceManagerStateReply()
        reply.success = result
        reply.version = version
        return reply

    def get_instance_manager_state(self) -> GetInstanceManagerStateReply:
        instances, version = self._get_instances()
        state = InstanceManagerState()
        state.version = version
        state.instances.extend(instances.values())
        reply = GetInstanceManagerStateReply()
        reply.state = state
        return reply

    def _get_instances(
        self, instance_ids: List[str]
    ) -> Tuple[Dict[str, Instance], int]:
        pairs, version = self._storage.get(self._table_name, instance_ids)
        instances = {}
        for instance_id, instance_data in pairs.items():
            instance = Instance()
            instance.ParseFromString(instance_data)
            instances[instance_id] = instance
        return instances, version

    def _transition_state(self, instance: Instance, new_state: int) -> bool:
        instance.status = new_state
        return True

    def _gc_stopped_nodes(self) -> bool:
        state = self.get_instance_manager_state().state
        instances = state.instances
        to_gc_instances = []
        to_gc_instance_ids = []
        for instance in instances:
            if (
                instance.status == Instance.STOPPED
                and instance.timestamp_since_last_state_change
                + self._stopped_node_gc_timeout_s
                < time.time()
            ):
                logger.info("GCing stopped node %s", instance.instance_id)
                to_gc_instance_ids.append(instance.instance_id)
                to_gc_instances.append(instance)
        if not to_gc_instances:
            return False

        result = self._storage.update(
            self._table_name, {}, to_gc_instances, state.version
        )[0]

        if result:
            self._notify_instances_status_changed(
                to_gc_instances,
                [Instance.STOPPED] * len(to_gc_instances),
                [Instance.GARBAGE_COLLECTED] * len(to_gc_instances),
            )
        return result

    def _notify_instances_status_changed(
        self,
        instances: List[Instance],
        old_status: List[Optional[int]],
        new_status: List[int],
    ) -> None:
        for i, instance in enumerate(instances):
            if instance.status == old_status[i]:
                instance.status = new_status[i]
            assert (
                instance.status == new_status
            ), f"instance {instance.instance_id} status is {instance.status}, expected {new_status}"
        self._status_change_subscriber.notify(
            [
                InstanceStatusChange(
                    instance=instance,
                    from_status=old_status[i],
                    to_status=new_status[i],
                )
                for i, instance in enumerate(instances)
            ]
        )
