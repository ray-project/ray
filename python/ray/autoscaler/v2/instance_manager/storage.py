import abc
import copy
from abc import abstractmethod
from threading import Lock
from typing import Dict, List, Optional, Set, Tuple


class Storage(metaclass=abc.ABCMeta):
    @abstractmethod
    def update(
        self,
        table: str,
        mutation: Dict[str, str],
        deletion: List[str],
        expected_version: Optional[int],
    ) -> Tuple[bool, int]:
        raise NotImplementedError("put() has to be implemented")

    @abstractmethod
    def get_all(self, table: str) -> Tuple[Dict[str, str], int]:
        raise NotImplementedError("put() has to be implemented")

    @abstractmethod
    def get(self, table: str, keys: List[str]) -> Tuple[Dict[str, str], int]:
        raise NotImplementedError("get() has to be implemented")

    @abstractmethod
    def get_version(self) -> int:
        raise NotImplementedError("get_version() has to be implemented")


class InMemoryStorage(Storage):
    def __init__(self):
        self._version = 0
        self._tables = {}
        self._lock = Lock()

    def update(
        self,
        table: str,
        mutation: Dict[str, str],
        deletion: List[str],
        expected_version: Optional[int],
    ) -> bool:
        with self._lock:
            if expected_version is not None and expected_version != self._version:
                return [False, self._version]
            self._version += 1
            self._tables[table] = self._tables.get(table, {}).update(mutation)
            for deleted_key in deletion:
                self._tables[table].pop(deleted_key, None)
            return [True, self._version]

    def get_all(self, table: str) -> Tuple[Dict[str, str], int]:
        with self._lock:
            return (copy.deepcopy(self._tables.get(table, {})), self._version)

    def get(self, table: str, keys: List[str]) -> Tuple[Dict[str, str], int]:
        with self._lock:
            result = {}
            for key in keys:
                if key in self._tables.get(table, {}):
                    result[key] = self._tables[table][key]
            return (result, self._version)

    def get_version(self) -> int:
        return self._version
