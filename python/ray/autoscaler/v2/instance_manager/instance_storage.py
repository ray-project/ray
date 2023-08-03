import copy
import logging
import time
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from ray.autoscaler.v2.instance_manager.storage import Storage, StoreStatus
from ray.core.generated.instance_manager_pb2 import Instance

logger = logging.getLogger(__name__)


@dataclass
class InstanceUpdateEvent:
    """Notifies the status change of an instance."""

    instance_id: str
    new_status: int
    new_ray_status: int = Instance.RAY_STATUS_UNKOWN


class InstanceUpdatedSuscriber(metaclass=ABCMeta):
    """Subscribers to instance status changes."""

    @abstractmethod
    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        pass


class InstanceStorage:
    """Instance storage stores the states of instances in the storage. It also
    allows users to subscribe to instance status changes to trigger reconciliation
    with cloud provider."""

    def __init__(
        self,
        cluster_id: str,
        storage: Storage,
        status_change_subscriber: Optional[InstanceUpdatedSuscriber] = None,
    ) -> None:
        self._storage = storage
        self._cluster_id = cluster_id
        self._table_name = f"instance_table@{cluster_id}"
        self._status_change_subscribers = []
        if status_change_subscriber:
            self._status_change_subscribers.append(status_change_subscriber)

    def add_status_change_subscriber(self, subscriber: InstanceUpdatedSuscriber):
        self._status_change_subscribers.append(subscriber)

    def batch_upsert_instances(
        self,
        updates: List[Instance],
        expected_storage_version: Optional[int] = None,
    ) -> StoreStatus:
        """Upsert instances into the storage. If the instance already exists,
        it will be updated. Otherwise, it will be inserted. If the
        expected_storage_version is specified, the update will fail if the
        current storage version does not match the expected version.

        Note the version of the upserted instances will be set to the current
        storage version.

        Args:
            updates: A list of instances to be upserted.
            expected_storage_version: The expected storage version.

        Returns:
            StoreStatus: A tuple of (success, storage_version).
        """
        mutations = {}
        version = self._storage.get_version()
        # handle version mismatch
        if expected_storage_version and expected_storage_version != version:
            return StoreStatus(False, version)

        for instance in updates:
            instance = copy.deepcopy(instance)
            # the instance version is set to 0, it will be
            # populated by the storage entry's verion on read
            instance.version = 0
            instance.timestamp_since_last_modified = int(time.time())
            mutations[instance.instance_id] = instance.SerializeToString()

        result, version = self._storage.batch_update(
            self._table_name, mutations, {}, expected_storage_version
        )

        if result:
            for subscriber in self._status_change_subscribers:
                subscriber.notify(
                    [
                        InstanceUpdateEvent(
                            instance_id=instance.instance_id,
                            new_status=instance.status,
                            new_ray_status=instance.ray_status,
                        )
                        for instance in updates
                    ],
                )

        return StoreStatus(result, version)

    def upsert_instance(
        self,
        instance: Instance,
        expected_instance_version: Optional[int] = None,
        expected_storage_verison: Optional[int] = None,
    ) -> StoreStatus:
        """Upsert an instance in the storage.
        If the expected_instance_version is specified, the update will fail
        if the current instance version does not match the expected version.
        Similarly, if the expected_storage_version is
        specified, the update will fail if the current storage version does not
        match the expected version.

        Note the version of the upserted instances will be set to the current
        storage version.

        Args:
            instance: The instance to be updated.
            expected_instance_version: The expected instance version.
            expected_storage_version: The expected storage version.

        Returns:
            StoreStatus: A tuple of (success, storage_version).
        """
        instance = copy.deepcopy(instance)
        # the instance version is set to 0, it will be
        # populated by the storage entry's verion on read
        instance.version = 0
        instance.timestamp_since_last_modified = int(time.time())
        result, version = self._storage.update(
            self._table_name,
            key=instance.instance_id,
            value=instance.SerializeToString(),
            expected_entry_version=expected_instance_version,
            expected_storage_version=expected_storage_verison,
            insert_only=False,
        )

        if result:
            for subscriber in self._status_change_subscribers:
                subscriber.notify(
                    [
                        InstanceUpdateEvent(
                            instance_id=instance.instance_id,
                            new_status=instance.status,
                            new_ray_status=instance.ray_status,
                        )
                    ],
                )

        return StoreStatus(result, version)

    def get_instances(
        self,
        instance_ids: List[str] = None,
        status_filter: Set[int] = None,
        ray_status_filter: Set[int] = None,
    ) -> Tuple[Dict[str, Instance], int]:
        """Get instances from the storage.

        Args:
            instance_ids: A list of instance ids to be retrieved. If empty, all
                instances will be retrieved.
            status_filter: Only instances with the specified status will be returned.
            ray_status_filter: Only instances with the specified ray status will
                be returned.

        Returns:
            Tuple[Dict[str, Instance], int]: A tuple of (instances, version).
                The instances is a dictionary of (instance_id, instance) pairs.
        """
        instance_ids = instance_ids or []
        status_filter = status_filter or set()
        pairs, version = self._storage.get(self._table_name, instance_ids)
        instances = {}
        for instance_id, (instance_data, entry_version) in pairs.items():
            instance = Instance()
            instance.ParseFromString(instance_data)
            instance.version = entry_version
            if status_filter and instance.status not in status_filter:
                continue
            if ray_status_filter and instance.ray_status not in ray_status_filter:
                continue
            instances[instance_id] = instance
        return instances, version

    def batch_delete_instances(
        self, instance_ids: List[str], expected_storage_version: Optional[int] = None
    ) -> StoreStatus:
        """Delete instances from the storage. If the expected_version is
        specified, the update will fail if the current storage version does not
        match the expected version.

        Args:
            to_delete: A list of instances to be deleted.
            expected_version: The expected storage version.

        Returns:
            StoreStatus: A tuple of (success, storage_version).
        """
        version = self._storage.get_version()
        if expected_storage_version and expected_storage_version != version:
            return StoreStatus(False, version)

        result = self._storage.batch_update(
            self._table_name, {}, instance_ids, expected_storage_version
        )

        if result[0]:
            for subscriber in self._status_change_subscribers:
                subscriber.notify(
                    [
                        InstanceUpdateEvent(
                            instance_id=instance_id,
                            new_status=Instance.GARBAGE_COLLECTED,
                            new_ray_status=Instance.RAY_STATUS_UNKOWN,
                        )
                        for instance_id in instance_ids
                    ],
                )
        return result
