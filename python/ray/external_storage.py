import abc
import os
from typing import List
import ray


class ExternalStorage(metaclass=abc.ABCMeta):
    """The base class for external storage.

    This class provides some useful functions for zero-copy object
    put/get from plasma store. Also it specifies the interface for
    object spilling.
    """

    def _get_objects_from_store(self, object_refs):
        worker = ray.worker.global_worker
        ray_object_pairs = worker.core_worker.get_objects(
            object_refs,
            worker.current_task_id,
            timeout_ms=0,
            plasma_objects_only=True)
        return ray_object_pairs

    def _put_object_to_store(self, metadata, data_size, file_like, object_ref):
        worker = ray.worker.global_worker
        worker.core_worker.put_file_like_object(metadata, data_size, file_like,
                                                object_ref)

    @abc.abstractmethod
    def spill_objects(self, object_refs):
        """Spill objects to the external storage. Objects are specified
        by their object refs.

        Args:
            object_refs: The list of the refs of the objects to be spilled.
        Returns:
            A list of keys corresponding to the input object refs.
        """

    @abc.abstractmethod
    def restore_spilled_objects(self, keys: List[bytes]):
        """Spill objects to the external storage. Objects are specified
        by their object refs.

        Args:
            keys: A list of bytes corresponding to the spilled objects.
        """


class NullStorage(ExternalStorage):
    """The class that represents an uninitialized external storage."""

    def spill_objects(self, object_refs):
        raise NotImplementedError("External storage is not initialized")

    def restore_spilled_objects(self, keys):
        raise NotImplementedError("External storage is not initialized")


class FileSystemStorage(ExternalStorage):
    """The class for filesystem-like external storage."""

    def __init__(self, directory_path):
        self.directory_path = directory_path
        self.prefix = "ray_spilled_object_"

    def spill_objects(self, object_refs):
        keys = []
        ray_object_pairs = self._get_objects_from_store(object_refs)
        for ref, (buf, metadata) in zip(object_refs, ray_object_pairs):
            filename = self.prefix + ref.hex()
            with open(os.path.join(self.directory_path, filename), "wb") as f:
                metadata_len = len(metadata)
                buf_len = len(buf)
                f.write(metadata_len.to_bytes(8, byteorder="little"))
                f.write(buf_len.to_bytes(8, byteorder="little"))
                f.write(metadata)
                f.write(memoryview(buf))
            keys.append(filename.encode())
        return keys

    def restore_spilled_objects(self, keys):
        for k in keys:
            filename = k.decode()
            ref = ray.ObjectRef(bytes.fromhex(filename[len(self.prefix):]))
            with open(os.path.join(self.directory_path, filename), "rb") as f:
                metadata_len = int.from_bytes(f.read(8), byteorder="little")
                buf_len = int.from_bytes(f.read(8), byteorder="little")
                metadata = f.read(metadata_len)
                # read remaining data to our buffer
                self._put_object_to_store(metadata, buf_len, f, ref)


_external_storage = NullStorage()


def setup_external_storage(config):
    """Setup the external storage according to the config."""
    global _external_storage
    if config:
        storage_type = config["type"]
        if storage_type == "filesystem":
            _external_storage = FileSystemStorage(**config["params"])
        else:
            raise ValueError(f"Unknown external storage type: {storage_type}")
    else:
        _external_storage = NullStorage()


def spill_objects(object_refs):
    """Spill objects to the external storage. Objects are specified
    by their object refs.

    Args:
        object_refs: The list of the refs of the objects to be spilled.
    Returns:
        A list of keys corresponding to the input object refs.
    """
    return _external_storage.spill_objects(object_refs)


def restore_spilled_objects(keys: List[bytes]):
    """Spill objects to the external storage. Objects are specified
    by their object refs.

    Args:
        keys: A list of bytes corresponding to the spilled objects.
    """
    _external_storage.restore_spilled_objects(keys)
