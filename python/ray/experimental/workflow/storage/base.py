import abc
from abc import abstractmethod
from typing import Any, List


class DataLoadError(Exception):
    pass


class DataSaveError(Exception):
    pass


class KeyNotFoundError(KeyError):
    pass


class Storage(metaclass=abc.ABCMeta):
    """Abstract base class for the low-level workflow storage.
    This class only provides low level primitives, e.g. save a certain
    type of object.
    """

    @abstractmethod
    def make_key(self, *names: str) -> str:
        """Make key from name sections."""

    @abstractmethod
    async def put(self, key: str, data: Any, is_json: bool = False) -> None:
        """Put object into storage.

        Args:
            key: The key of the object.
            data: The object data.
            is_json: True if the object is a json object.
        """

    @abstractmethod
    async def get(self, key: str, is_json: bool = False) -> Any:
        """Get object from storage.

        Args:
            key: The key of the object.
            is_json: True if the object is a json object.

        Returns:
            The object from storage.
        """

    @abstractmethod
    async def delete_prefix(self, key_prefix: str) -> None:
        """Delete an object with prefix.

        Args:
            key_prefix: The prefix to delete.
        """

    @abstractmethod
    async def scan_prefix(self, key_prefix: str) -> List[str]:
        """List all keys with the prefix.

        Args:
            key_prefix: The prefix of the key.

        Returns:
           List of matched keys.
        """

    @property
    @abstractmethod
    def storage_url(self) -> str:
        """Get the URL of the storage."""

    @abstractmethod
    def __reduce__(self):
        """Reduce the storage to a serializable object."""
