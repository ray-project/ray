import abc

import ray


class Checkpoint(abc.ABC):
    """Checkpoint interface"""

    @classmethod
    def from_bytes(cls, data: bytes) -> "Checkpoint":
        """Create checkpoint object from bytes stream.

        Args:
            data (bytes): Data stream containing pickled checkpoint data.

        Returns:
            Checkpoint: checkpoint object.
        """
        raise NotImplementedError

    def to_bytes(self) -> bytes:
        """Return Checkpoint serialized as bytes stream.

        Returns:
            bytes: Bytes stream containing checkpoint data.
        """
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

    def to_directory(self, path: str) -> str:
        """Write checkpoint data to directory.

        Args:
            path (str): Target directory to restore data in.

        Returns:
            str: Directory containing checkpoint data.
        """
        raise NotImplementedError

    @classmethod
    def from_cloud_storage(cls, location: str) -> "Checkpoint":
        """Create checkpoint object from cloud storage location.

        Args:
            location (str): Cloud storage location (URI).

        Returns:
            Checkpoint: checkpoint object.
        """
        raise NotImplementedError

    def to_cloud_storage(self, location: str) -> str:
        """Write checkpoint data to cloud location.

        ARgs:
            location (str): Target cloud location to write data to.

        Returns:
            str: Cloud location containing checkpoint data.
        """
        raise NotImplementedError
