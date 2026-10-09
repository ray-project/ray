import copy
from abc import ABCMeta, abstractmethod
from collections import defaultdict, namedtuple
from threading import Lock
from typing import Dict, List, Optional, Tuple

StoreStatus = namedtuple("StoreStatus", ["success", "version"])
VersionedValue = namedtuple("VersionedValue", ["value", "version"])


class Storage(metaclass=ABCMeta):
    """Interface for a storage backend that stores the state of nodes in the cluster.

    The storage is thread-safe.

    The storage is versioned, which means that each successful stage change to the
    storage will bump the version number. The version number can be used to
    implement optimistic concurrency control.

    Each entry in the storage table is also versioned. The version number of an entry
    is the last version number when the entry is updated.
    """

    @abstractmethod
    def batch_update(
        self,
        table: str,
        mutation: Optional[Dict[str, str]] = None,
        deletion: Optional[List[str]] = None,
        expected_storage_version: Optional[int] = None,
    ) -> StoreStatus:
        """Batch update the storage table. This method is atomic.

        Args:
            table: The name of the table.
            mutation: A dictionary of key-value pairs to be updated.
            deletion: A list of keys to be deleted.
            expected_storage_version: The expected storage version. The
                update will fail if the version does not match the
                current storage version.

        Returns:
            StoreStatus: A tuple of (success, version). If the update is
                successful, returns (True, new_version).
                Otherwise, returns (False, current_version).
        """
        raise NotImplementedError("batch_update() has to be implemented")

    @abstractmethod
    def update(
        self,
        table: str,
        key: str,
        value: str,
        expected_entry_version: Optional[int] = None,
        insert_only: bool = False,
    ) -> StoreStatus:
        """Update a single entry in the storage table.

        Args:
            table: The name of the table.
            key: The key of the entry.
            value: The value of the entry.
            expected_entry_version: The expected version of the entry.
                The update will fail if the version does not match the current
                version of the entry.
            insert_only: If True, the update will
                fail if the entry already exists.
        Returns:
            StoreStatus: A tuple of (success, version). If the update is
                successful, returns (True, new_version). Otherwise,
                returns (False, current_version).
        """
        raise NotImplementedError("update() has to be implemented")

    @abstractmethod
    def get_all(self, table: str) -> Tuple[Dict[str, Tuple[str, int]], int]:
        raise NotImplementedError("get_all() has to be implemented")

    @abstractmethod
    def get(
        self, table: str, keys: List[str]
    ) -> Tuple[Dict[str, Tuple[str, int]], int]:
        """Get a list of entries from the storage table.

        Args:
            table: The name of the table.
            keys: A list of keys to be retrieved. If the list is empty,
                all entries in the table will be returned.

        Returns:
            Tuple[Dict[str, VersionedValue], int]: A tuple of
                (entries, storage_version). The entries is a dictionary of
                (key, (value, entry_version)) pairs. The entry_version is the
                version of the entry when it was last updated. The
                storage_version is the current storage version.
        """
        raise NotImplementedError("get() has to be implemented")

    @abstractmethod
    def get_version(self) -> int:
        """Get the current storage version.

        Returns:
            int: The current storage version.
        """
        raise NotImplementedError("get_version() has to be implemented")


class InMemoryStorage(Storage):
    """An in-memory implementation of the Storage interface. This implementation
    is not durable"""

    def __init__(self):
        self._version = 0
        self._tables = defaultdict(dict)
        self._lock = Lock()

    def batch_update(
        self,
        table: str,
        mutation: Dict[str, str] = None,
        deletion: List[str] = None,
        expected_version: Optional[int] = None,
    ) -> StoreStatus:
        mutation = mutation if mutation else {}
        deletion = deletion if deletion else []
        with self._lock:
            if expected_version is not None and expected_version != self._version:
                return StoreStatus(False, self._version)
            self._version += 1
            key_value_pairs_with_version = {
                key: VersionedValue(value, self._version)
                for key, value in mutation.items()
            }
            self._tables[table].update(key_value_pairs_with_version)
            for deleted_key in deletion:
                self._tables[table].pop(deleted_key, None)
            return StoreStatus(True, self._version)

    def update(
        self,
        table: str,
        key: str,
        value: str,
        expected_entry_version: Optional[int] = None,
        expected_storage_version: Optional[int] = None,
        insert_only: bool = False,
    ) -> StoreStatus:
        with self._lock:
            if (
                expected_storage_version is not None
                and expected_storage_version != self._version
            ):
                return StoreStatus(False, self._version)
            if insert_only and key in self._tables[table]:
                return StoreStatus(False, self._version)
            _, version = self._tables[table].get(key, (None, -1))
            if expected_entry_version is not None and expected_entry_version != version:
                return StoreStatus(False, self._version)
            self._version += 1
            self._tables[table][key] = VersionedValue(value, self._version)
            return StoreStatus(True, self._version)

    def get_all(self, table: str) -> Tuple[Dict[str, VersionedValue], int]:
        with self._lock:
            return (copy.deepcopy(self._tables[table]), self._version)

    def get(self, table: str, keys: List[str]) -> Tuple[Dict[str, VersionedValue], int]:
        if not keys:
            return self.get_all(table)
        with self._lock:
            result = {}
            for key in keys:
                if key in self._tables.get(table, {}):
                    result[key] = self._tables[table][key]
            return StoreStatus(result, self._version)

    def get_version(self) -> int:
        return self._version
