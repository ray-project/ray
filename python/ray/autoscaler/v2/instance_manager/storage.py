import copy
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from threading import Lock
from typing import Dict, List, Optional, Tuple


class Storage(metaclass=ABCMeta):
    @abstractmethod
    def batch_update(
        self,
        table: str,
        mutation: Dict[str, str] = {},
        deletion: List[str] = [],
        expected_version: Optional[int] = None,
    ) -> Tuple[bool, int]:
        raise NotImplementedError("batch_update() has to be implemented")

    @abstractmethod
    def update(
        self,
        table: str,
        key: str,
        value: str,
        expected_entry_version: Optional[int] = None,
        insert_only: bool = False,
    ) -> Tuple[bool, int]:
        raise NotImplementedError("update() has to be implemented")

    @abstractmethod
    def get_all(self, table: str) -> Tuple[Dict[str, Tuple[str, int]], int]:
        raise NotImplementedError("get_all() has to be implemented")

    @abstractmethod
    def get(
        self, table: str, keys: List[str]
    ) -> Tuple[Dict[str, Tuple[str, int]], int]:
        raise NotImplementedError("get() has to be implemented")

    @abstractmethod
    def get_version(self) -> int:
        raise NotImplementedError("get_version() has to be implemented")


class InMemoryStorage(Storage):
    def __init__(self):
        self._version = 0
        self._tables = defaultdict(dict)
        self._lock = Lock()

    def batch_update(
        self,
        table: str,
        mutation: Dict[str, str] = {},
        deletion: List[str] = [],
        expected_version: Optional[int] = None,
    ) -> Tuple[bool, int]:
        with self._lock:
            if expected_version is not None and expected_version != self._version:
                return [False, self._version]
            self._version += 1
            key_value_pairs_with_version = {
                key: (value, self._version) for key, value in mutation.items()
            }
            self._tables[table].update(key_value_pairs_with_version)
            for deleted_key in deletion:
                self._tables[table].pop(deleted_key, None)
            return [True, self._version]

    def update(
        self,
        table: str,
        key: str,
        value: str,
        expected_entry_version: Optional[int] = None,
        expected_storage_version: Optional[int] = None,
        insert_only: bool = False,
    ) -> Tuple[bool, int]:
        with self._lock:
            if (
                expected_storage_version is not None
                and expected_storage_version != self._version
            ):
                return [False, self._version]
            if insert_only and key in self._tables[table]:
                return [False, self._version]
            _, version = self._tables[table].get(key, (None, -1))
            if expected_entry_version is not None and expected_entry_version != version:
                return [False, self._version]
            self._version += 1
            self._tables[table][key] = (value, self._version)
            return [True, self._version]

    def get_all(self, table: str) -> Tuple[Dict[str, Tuple[str, int]], int]:
        with self._lock:
            return (copy.deepcopy(self._tables[table]), self._version)

    def get(
        self, table: str, keys: List[str]
    ) -> Tuple[Dict[str, Tuple[str, int]], int]:
        if not keys:
            return self.get_all(table)
        with self._lock:
            result = {}
            for key in keys:
                if key in self._tables.get(table, {}):
                    result[key] = self._tables[table][key]
            return (result, self._version)

    def get_version(self) -> int:
        return self._version
