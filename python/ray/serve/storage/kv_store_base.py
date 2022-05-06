import abc
from abc import abstractmethod
from typing import Optional

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class KVStoreBase(metaclass=abc.ABCMeta):
    """Abstract class for KVStore defining APIs needed for ray serve
    use cases, currently (8/6/2021) controller state checkpointing.
    """

    @abstractmethod
    def get_storage_key(self, key: str) -> str:
        """Get internal key for storage.

        Args:
            key (str): User provided key

        Returns:
            storage_key (str): Formatted key for storage, usually by
                prepending namespace.
        """
        raise NotImplementedError("get_storage_key() has to be implemented")

    @abstractmethod
    def put(self, key: str, val: bytes) -> bool:
        """Put object into kv store, bytes only.

        Args:
            key (str): Key for object to be stored.
            val (bytes): Byte value of object.
        """
        raise NotImplementedError("put() has to be implemented")

    @abstractmethod
    def get(self, key: str) -> Optional[bytes]:
        """Get object from storage.

        Args:
            key (str): Key for object to be retrieved.

        Returns:
            val (bytes): Byte value of object from storage.
        """
        raise NotImplementedError("get() has to be implemented")

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete an object.

        Args:
            key (str): Key for object to be deleted.
        """
        raise NotImplementedError("delete() has to be implemented")
