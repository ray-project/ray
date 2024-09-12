import copy
import logging
from typing import Dict, List, Optional, Set, Tuple

from ray.autoscaler.v2.instance_manager.storage import Storage, StoreStatus
from ray.core.generated.instance_manager_pb2 import Instance

logger = logging.getLogger(__name__)


class InstanceStorage:
    """Instance storage stores the states of instances in the storage."""

    def __init__(
        self,
        cluster_id: str,
        storage: Storage,
    ) -> None:
        self._storage = storage
        self._cluster_id = cluster_id
        self._table_name = f"instance_table@{cluster_id}"

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
            mutations[instance.instance_id] = instance.SerializeToString()

        result, version = self._storage.batch_update(
            self._table_name, mutations, {}, expected_storage_version
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
        result, version = self._storage.update(
            self._table_name,
            key=instance.instance_id,
            value=instance.SerializeToString(),
            expected_entry_version=expected_instance_version,
            expected_storage_version=expected_storage_verison,
            insert_only=False,
        )

        return StoreStatus(result, version)

    def get_instances(
        self,
        instance_ids: List[str] = None,
        status_filter: Set[int] = None,
    ) -> Tuple[Dict[str, Instance], int]:
        """Get instances from the storage.

        Args:
            instance_ids: A list of instance ids to be retrieved. If empty, all
                instances will be retrieved.
            status_filter: Only instances with the specified status will be returned.

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
        return result
