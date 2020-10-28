import abc
import os
from typing import List

import ray


class ExternalStorageConfigInvalidError(Exception):
    """Exception that indicates external storage is not properly setup.

    This exception should be thrown when external storage setup is not
    successful. For example, if the directory path for file system object
    spilling doesn't exist, that means object spilling wouldn't work. In
    that case, this exception should be thrown. This should be thrown
    only from __init__ method inside External storage inherited classes.
    """
    pass


class ExternalStorage(metaclass=abc.ABCMeta):
    """The base class for external storage.

    This class provides some useful functions for zero-copy object
    put/get from plasma store. Also it specifies the interface for
    object spilling.

    When inheriting this class, please make sure to implement validation
    logic inside __init__ method. When ray instance starts, it will
    instantiating external storage to validate the config.

    Raises:
        ExternalStorageConfigInvalidError: when given configuration for
            the external storage is invalid.
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

    def get_stats(self):
        raise NotImplementedError("External storage is not initialized")


class FileSystemStorage(ExternalStorage):
    """The class for filesystem-like external storage.

    Raises:
        ExternalStorageConfigInvalidError: Raises directory path to
            spill objects doesn't exist.
    """

    def __init__(self, directory_path):
        self.directory_path = directory_path
        self.prefix = "ray_spilled_object_"
        if not os.path.exists(self.directory_path):
            raise ExternalStorageConfigInvalidError(
                "The given directory path to store objects, "
                f"{self.directory_path}, doesn't exist.")

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


class ExternalStorageSmartOpenImpl(ExternalStorage):
    """The external storage class implemented by smart_open.
    (https://github.com/RaRe-Technologies/smart_open)

    Smart open supports multiple backend with the same APIs.

    Args:
        uri(str): Storage URI used for smart open.
        prefix(str): Prefix of objects that are stored.
        override_transport_params(dict): Overriding the default value of
            transport_params for smart-open library.

    Raises:
        ExternalStorageConfigInvalidError: If it fails to setup.
            For example, if smart open library
            is not downloaded, this will fail.
    """

    def __init__(self,
                 uri: str,
                 prefix: str = "ray_spilled_object_",
                 override_transport_params: dict = None):
        try:
            from smart_open import open  # noqa
        except ModuleNotFoundError:
            raise ExternalStorageConfigInvalidError(
                "S3 storage is chosen to be a object spilling "
                "external storage, but smart_open "
                "is not downloaded.")

        self.uri = uri.strip("/")
        self.prefix = prefix
        self.override_transport_params = override_transport_params or {}
        self.transport_params = {}.update(self.override_transport_params)

    def spill_objects(self, object_refs):
        keys = []
        ray_object_pairs = self._get_objects_from_store(object_refs)
        for ref, (buf, metadata) in zip(object_refs, ray_object_pairs):
            key = self.prefix + ref.hex()
            self._spill_object(key, ref, buf, metadata)
            keys.append(key.encode())
        return keys

    def restore_spilled_objects(self, keys):
        for k in keys:
            key = k.decode()
            ref = ray.ObjectRef(bytes.fromhex(key[len(self.prefix):]))
            self._restore_spilled_object(key, ref)

    def _spill_object(self, key, ref, buf, metadata):
        from smart_open import open
        with open(
                self._build_uri(key), "wb",
                transport_params=self.transport_params) as file_like:
            metadata_len = len(metadata)
            buf_len = len(buf)
            file_like.write(metadata_len.to_bytes(8, byteorder="little"))
            file_like.write(buf_len.to_bytes(8, byteorder="little"))
            file_like.write(metadata)
            file_like.write(memoryview(buf))

    def _restore_spilled_object(self, key, ref):
        from smart_open import open
        with open(
                self._build_uri(key), "rb",
                transport_params=self.transport_params) as file_like:
            metadata_len = int.from_bytes(
                file_like.read(8), byteorder="little")
            buf_len = int.from_bytes(file_like.read(8), byteorder="little")
            metadata = file_like.read(metadata_len)
            # read remaining data to our buffer
            self._put_object_to_store(metadata, buf_len, file_like, ref)

    def _build_uri(self, key):
        return f"{self.uri}/{key}"


_external_storage = NullStorage()


def setup_external_storage(config):
    """Setup the external storage according to the config."""
    global _external_storage
    if config:
        storage_type = config["type"]
        if storage_type == "filesystem":
            _external_storage = FileSystemStorage(**config["params"])
        elif storage_type == "smart_open":
            _external_storage = ExternalStorageSmartOpenImpl(
                **config["params"])
        else:
            raise ValueError(f"Unknown external storage type: {storage_type}")
    else:
        _external_storage = NullStorage()


def reset_external_storage():
    global _external_storage
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
