import logging
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set

from ray.autoscaler.v2.instance_manager.storage import Storage
from ray.core.generated.instance_manager_pb2 import (
    Instance,
)

logger = logging.getLogger(__name__)


@dataclass
class InstanceUpdateEvent:
    instance_id: str
    old: Optional[Instance]
    new: Optional[Instance]


class InstanceUpdatedSuscriber(metaclass=ABCMeta):
    @abstractmethod
    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        pass


class InstanceStorage(object):
    def __init__(
        self,
        cluster_id: str,
        storage: Storage,
        status_change_subscriber: Optional[InstanceUpdatedSuscriber] = None,
    ) -> None:
        super().__init__()
        self._storage = storage
        self._cluster_id = cluster_id
        self._table_name = f"instance_table@{cluster_id}"
        self._status_change_subscriber = status_change_subscriber

    def upsert_instances(
        self,
        updates: List[Instance],
        expected_version: Optional[int],
    ) -> Tuple[bool, int]:
        mutations = {}

        old_instances, version = self.get_instances(
            [instance.instance_id for instance in updates]
        )

        # handle version mismatch
        if expected_version and expected_version != version:
            return False, version

        # handle teriminating instances
        for instance in updates.values():
            mutations[instance.instance_id] = instance.SerializeToString()

        result, version = self._storage.update(
            self._table_name, mutations, {}, expected_version
        )

        if result and self._status_change_subscriber:
            self._status_change_subscriber.notify(
                [
                    InstanceUpdateEvent(
                        instance_id=instance.instance_id,
                        old=old_instances.get(instance.instance_id),
                        new=instance,
                    )
                    for instance in updates
                ],
            )

        return result, version

    def get_instances(self, instance_ids: List[str]=[], status_filter: Set[int]={}) -> Tuple[Dict[str, Instance], int]:
        pairs, version = self._storage.get(self._table_name, instance_ids)
        instances = {}
        for instance_id, instance_data in pairs.items():
            instance = Instance()
            instance.ParseFromString(instance_data)
            if status_filter and instance.status not in status_filter:
                continue
            instances[instance_id] = instance
        return instances, version

    def delete_instances(
        self, to_delete: List[Instance], expected_version: Optional[int]
    ) -> Tuple[bool, int]:
        old_instances, version = self.get_instances(
            [instance.instance_id for instance in to_delete]
        )
        if expected_version and expected_version != version:
            return False, version

        result = self._storage.update(self._table_name, {}, to_delete, expected_version)

        if result[0] and self._status_change_subscriber:
            self._status_change_subscriber.notify(
                [
                    InstanceUpdateEvent(
                        instance_id=instance.instance_id,
                        old=old_instances.get(instance.instance_id),
                        new=None,
                    )
                    for instance in to_delete
                ],
            )
        return result