import abc
from typing import Dict, Optional

import ray


class Checkpoint(abc.ABC):
    """Checkpoint interface"""

    def __init__(self, metadata: Optional[Dict] = None):
        self.metadata = metadata or {}

    @classmethod
    def from_bytes(cls, data: bytes) -> "Checkpoint":
        """Create checkpoint object from bytes object.

        Args:
            data (bytes): Data object containing pickled checkpoint data.

        Returns:
            Checkpoint: checkpoint object.
        """
        raise NotImplementedError

    def to_bytes(self) -> bytes:
        """Return Checkpoint serialized as bytes object.

        Returns:
            bytes: Bytes object containing checkpoint data.
        """
        # Todo: Add support for stream in the future (to_bytes(file_like))
        raise NotImplementedError

    @classmethod
    def from_dict(cls, data: dict) -> "Checkpoint":
        """Create checkpoint object from dictionary.

        Args:
            data (dict): Dictionary containing checkpoint data.

        Returns:
            Checkpoint: checkpoint object.
        """
        raise NotImplementedError

    def to_dict(self) -> dict:
        """Return checkpoint data as dictionary.

        Returns:
            dict: Dictionary containing checkpoint data.
        """

    @classmethod
    def from_object_ref(cls, obj_ref: ray.ObjectRef) -> "Checkpoint":
        """Create checkpoint object from object reference.

        Args:
            obj_ref (ray.ObjectRef): ObjectRef pointing to checkpoint data.

        Returns:
            Checkpoint: checkpoint object.
        """
        raise NotImplementedError

    def to_object_ref(self) -> ray.ObjectRef:
        """Return checkpoint data as object reference.

        Returns:
            ray.ObjectRef: ObjectRef pointing to checkpoint data.
        """
        raise NotImplementedError

    @classmethod
    def from_directory(cls, path: str) -> "Checkpoint":
        """Create checkpoint object from directory.

        Args:
            path (str): Directory containing checkpoint data.

        Returns:
            Checkpoint: checkpoint object.
        """
        raise NotImplementedError

    def to_directory(self, path: Optional[str] = None) -> str:
        """Write checkpoint data to directory.

        Args:
            path (str): Target directory to restore data in.

        Returns:
            str: Directory containing checkpoint data.
        """
        raise NotImplementedError

    @classmethod
    def from_uri(cls, location: str) -> "Checkpoint":
        """Create checkpoint object from location URI (e.g. cloud storage).

        Args:
            location (str): Source location URI to read data from.

        Returns:
            Checkpoint: checkpoint object.
        """
        raise NotImplementedError

    def to_uri(self, location: str) -> str:
        """Write checkpoint data to location URI (e.g. cloud storage).

        ARgs:
            location (str): Target location URI to write data to.

        Returns:
            str: Cloud location containing checkpoint data.
        """
        raise NotImplementedError
