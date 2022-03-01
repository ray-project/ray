import abc
import json
import shutil
import tarfile
import tempfile

import cloudpickle as pickle
import os
from typing import Any, Type, Optional, Dict

import ray
from ray.util.ml_utils.cloud import (
    upload_to_bucket,
    is_cloud_target,
    download_from_bucket,
)


def write_metadata(path: str, metadata: Dict[str, Any]) -> None:
    metadata_file = os.path.join(path, ".checkpoint_metadata")
    with open(metadata_file, "wt") as fp:
        json.dump(metadata, fp)


def load_metadata(path: str) -> Dict[str, Any]:
    metadata_file = os.path.join(path, ".checkpoint_metadata")
    if os.path.exists(metadata_file):
        with open(metadata_file, "rt") as fp:
            return json.load(fp)


def temporary_checkpoint_dir() -> str:
    return tempfile.mkdtemp(prefix="checkpoint_tmp_", dir=os.getcwd())


def _pack(path: str) -> bytes:
    _, tmpfile = tempfile.mkstemp()
    with tarfile.open(tmpfile, "w:gz") as tar:
        tar.add(path, arcname="")

    with open(tmpfile, "rb") as f:
        stream = f.read()

    os.remove(tmpfile)
    return stream


def _unpack(stream: bytes, path: str) -> str:
    _, tmpfile = tempfile.mkstemp()

    with open(tmpfile, "wb") as f:
        f.write(stream)

    with tarfile.open(tmpfile) as tar:
        tar.extractall(path)

    os.remove(tmpfile)
    return path


class Checkpoint(abc.ABC):
    """Checkpoint interface.

    This implementation provides interfaces to translate between
    different checkpoint storage locations: Local FS storage, remote
    node FS storage, data object, and cloud storage location.

    The following metadata keys are introduced for correct serialization
    and deserialization of checkpoints:

        is_fs_checkpoint (bool): If this is set, this checkpoint was
            created from a FS location (e.g. a directory) and can thus
            be deserialized into a FS location again for downstream processing.
        is_data_checkpoint (bool): If this is set, this checkpoint was
            created from a data object (e.g. a dictionary) and can
            thus be deserialized into a data checkpoint again for downstream
            processing.
    """

    def __init__(self, metadata: Optional[Dict] = None):
        self.metadata = metadata or {}

    def __eq__(self, other):
        return isinstance(other, Checkpoint) and self.metadata == other.metadata

    @classmethod
    def from_bytes(cls, data: bytes) -> "DataCheckpoint":
        """Create checkpoint object from bytes object.

        Args:
            data (bytes): Data object containing pickled checkpoint data.

        Returns:
            Checkpoint: checkpoint object.
        """
        bytes_data = pickle.loads(data)
        if isinstance(bytes_data, dict):
            data_dict = bytes_data
        else:
            data_dict = {"data": bytes_data}
        return cls.from_dict(data_dict)

    def to_bytes(self) -> bytes:
        """Return Checkpoint serialized as bytes object.

        Returns:
            bytes: Bytes object containing checkpoint data.
        """
        # Todo: Add support for stream in the future (to_bytes(file_like))
        return pickle.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict) -> "DataCheckpoint":
        """Create checkpoint object from dictionary.

        Args:
            data (dict): Dictionary containing checkpoint data.

        Returns:
            Checkpoint: checkpoint object.
        """
        metadata = data.get("metadata", {})
        if metadata.get("is_data_checkpoint", False):
            checkpoint_data = data["data"]
        else:
            checkpoint_data = data
        return DataCheckpoint(data=checkpoint_data, metadata=metadata)

    def to_dict(self) -> dict:
        """Return checkpoint data as dictionary.

        Returns:
            dict: Dictionary containing checkpoint data.
        """
        raise NotImplementedError

    @classmethod
    def from_object_ref(cls, obj_ref: ray.ObjectRef) -> "ObjectStoreCheckpoint":
        """Create checkpoint object from object reference.

        Args:
            obj_ref (ray.ObjectRef): ObjectRef pointing to checkpoint data.

        Returns:
            Checkpoint: checkpoint object.
        """
        return ObjectStoreCheckpoint(obj_ref=obj_ref, metadata=None)

    def to_object_ref(self) -> ray.ObjectRef:
        """Return checkpoint data as object reference.

        Returns:
            ray.ObjectRef: ObjectRef pointing to checkpoint data.
        """
        return ray.put(self.to_dict())

    @classmethod
    def from_directory(cls, path: str) -> "LocalStorageCheckpoint":
        """Create checkpoint object from directory.

        Args:
            path (str): Directory containing checkpoint data.

        Returns:
            Checkpoint: checkpoint object.
        """
        metadata = load_metadata(path)

        local_checkpoint = LocalStorageCheckpoint(path=path, metadata=metadata)
        return local_checkpoint

    def to_directory(self, path: Optional[str] = None) -> str:
        """Write checkpoint data to directory.

        Args:
            path (str): Target directory to restore data in.

        Returns:
            str: Directory containing checkpoint data.
        """
        raise NotImplementedError

    @classmethod
    def from_uri(cls, location: str) -> "ExternalStorageCheckpoint":
        """Create checkpoint object from location URI (e.g. cloud storage).

        Args:
            location (str): Source location URI to read data from.

        Returns:
            Checkpoint: checkpoint object.
        """
        return ExternalStorageCheckpoint(location=location)

    def to_uri(self, location: str) -> str:
        """Write checkpoint data to location URI (e.g. cloud storage).

        ARgs:
            location (str): Target location URI to write data to.

        Returns:
            str: Cloud location containing checkpoint data.
        """
        path = self.to_directory(path=None)
        assert is_cloud_target(location)
        upload_to_bucket(bucket=location, local_path=path)
        if not isinstance(self, FSStorageCheckpoint):
            # Only delete directory if it was a temporary directory
            shutil.rmtree(path)
        return location

    def to_data_checkpoint(self) -> "DataCheckpoint":
        raise NotImplementedError

    def to_object_store_checkpoint(self) -> "ObjectStoreCheckpoint":
        raise NotImplementedError

    def to_local_storage_checkpoint(
        self, path: Optional[str] = None
    ) -> "LocalStorageCheckpoint":
        raise NotImplementedError

    def to_external_storage_checkpoint(
        self, location: str
    ) -> "ExternalStorageCheckpoint":
        location = self.to_uri(location=location)
        return ExternalStorageCheckpoint(location=location)


class DataCheckpoint(Checkpoint):
    def __init__(self, data: Any, metadata: Optional[Dict] = None):
        Checkpoint.__init__(self, metadata=metadata)
        self.data = data

    @property
    def is_fs_checkpoint(self) -> bool:
        return self.metadata.get("is_fs_checkpoint", False)

    def to_dict(self) -> dict:
        if isinstance(self.data, dict):
            return self.data
        else:
            return {
                "data": self.data,
                "metadata": {**self.metadata, "is_data_checkpoint": True},
            }

    def to_directory(self, path: Optional[str] = None) -> str:
        path = path if path is not None else temporary_checkpoint_dir()

        new_metadata = self.metadata.copy()

        os.makedirs(path, exist_ok=True)
        # Drop marker
        open(os.path.join(path, ".is_checkpoint"), "a").close()
        if self.is_fs_checkpoint:
            # Recover FS checkpoint from data
            _unpack(self.data["data"], path)
            new_metadata["is_data_checkpoint"] = False
        else:
            # Dump into checkpoint.pkl
            checkpoint_data_path = os.path.join(path, "checkpoint.pkl")
            with open(checkpoint_data_path, "wb") as f:
                pickle.dump(self.data, f)
            new_metadata["is_data_checkpoint"] = True

        write_metadata(path, new_metadata)
        return path

    def to_data_checkpoint(self) -> "DataCheckpoint":
        return self

    def to_local_storage_checkpoint(
        self, path: Optional[str] = None
    ) -> "LocalStorageCheckpoint":
        """Convert DataCheckpoint to LocalStorageCheckpoint"""
        checkpoint_path = self.to_directory(path)
        metadata = load_metadata(checkpoint_path)

        local_checkpoint = LocalStorageCheckpoint(
            path=checkpoint_path, metadata=metadata
        )
        return local_checkpoint

    def to_object_store_checkpoint(self) -> "ObjectStoreCheckpoint":
        return ObjectStoreCheckpoint(self.to_object_ref(), metadata=self.metadata)

    def __eq__(self, other):
        return (
            isinstance(other, DataCheckpoint)
            and Checkpoint.__eq__(self, other)
            and self.data == other.data
        )


class ObjectStoreCheckpoint(Checkpoint):
    def __init__(self, obj_ref: ray.ObjectRef, metadata: Any):
        Checkpoint.__init__(self, metadata=metadata)
        self.obj_ref = obj_ref

    def to_dict(self) -> dict:
        data = ray.get(self.obj_ref)
        return Checkpoint.from_dict(data).to_dict()

    def to_directory(self, path: Optional[str] = None) -> str:
        data = ray.get(self.obj_ref)
        checkpoint = DataCheckpoint.from_dict(data)
        return checkpoint.to_directory(path)

    def to_data_checkpoint(self) -> "DataCheckpoint":
        data = ray.get(self.obj_ref)
        return DataCheckpoint.from_dict(data)

    def to_object_store_checkpoint(self) -> "ObjectStoreCheckpoint":
        return self

    def to_local_storage_checkpoint(
        self, path: Optional[str] = None
    ) -> "LocalStorageCheckpoint":
        data = ray.get(self.obj_ref)
        checkpoint = DataCheckpoint.from_dict(data)
        return checkpoint.to_local_storage_checkpoint(path)

    def __eq__(self, other):
        return (
            isinstance(other, ObjectStoreCheckpoint)
            and Checkpoint.__eq__(self, other)
            and self.obj_ref == other.obj_ref
        )


class FSStorageCheckpoint(Checkpoint, abc.ABC):
    def __init__(self, path: str, node_ip: str, metadata: Optional[dict] = None):
        Checkpoint.__init__(self, metadata=metadata)
        self.path = path
        self.node_ip = node_ip

    @property
    def is_data_checkpoint(self) -> bool:
        """Return True if this can be converted back to data checkpoint."""
        return self.metadata.get("is_data_checkpoint", False)

    def to_directory(self, path: Optional[str] = None) -> str:
        if path is not None:
            if os.path.exists(path):
                shutil.rmtree(path)
            shutil.copytree(self.path, path)

        return self.path

    def to_dict(self) -> dict:
        if self.is_data_checkpoint:
            # Restore previous DataCheckpoint from disk
            checkpoint_data_path = os.path.join(self.path, "checkpoint.pkl")
            with open(checkpoint_data_path, "rb") as f:
                data = pickle.load(f)
            return data

        # Else, pickle whole directory
        data = _pack(self.path)

        checkpoint_data = {
            "data": data,
            "metadata": {**self.metadata, "is_fs_checkpoint": True},
        }

        return checkpoint_data

    def to_data_checkpoint(self) -> "DataCheckpoint":
        """Convert FSStorageCheckpoint to DataCheckpoint"""
        full_data_dict = self.to_dict()
        if self.is_data_checkpoint:
            data = full_data_dict
            metadata = {}
        else:
            data = full_data_dict.get("data", {})
            metadata = full_data_dict.get("metadata", {})
        return DataCheckpoint(data=data, metadata=metadata)

    def to_object_store_checkpoint(self) -> "ObjectStoreCheckpoint":
        data_checkpoint = self.to_data_checkpoint()
        return data_checkpoint.to_object_store_checkpoint()

    def __reduce_ex__(self, protocol):
        if self.node_ip == ray.util.get_node_ip_address():
            return LocalStorageCheckpoint, (self.path, self.metadata)
        else:
            return RemoteNodeStorageCheckpoint, (self.path, self.node_ip, self.metadata)

    def __eq__(self, other):
        return (
            isinstance(other, FSStorageCheckpoint)
            and Checkpoint.__eq__(self, other)
            and self.path == other.path
            and self.node_ip == other.node_ip
        )

    def __repr__(self):
        return f"<{self.__class__.__name__} path={self.path} node_ip={self.node_ip}>"


class LocalStorageCheckpoint(FSStorageCheckpoint):
    def __init__(self, path: str, metadata: Optional[Dict] = None):
        FSStorageCheckpoint.__init__(
            self, path=path, node_ip=ray.util.get_node_ip_address(), metadata=metadata
        )

    def to_local_storage_checkpoint(
        self, path: Optional[str] = None
    ) -> "LocalStorageCheckpoint":
        if path == self.path or path is None:
            return self

        if path and os.path.exists(path):
            shutil.rmtree(path)
        shutil.copytree(self.path, path)
        return LocalStorageCheckpoint(path, metadata=self.metadata)

    def __eq__(self, other):
        return isinstance(other, LocalStorageCheckpoint) and FSStorageCheckpoint.__eq__(
            self, other
        )


class RemoteNodeStorageCheckpoint(FSStorageCheckpoint):
    def __init__(self, path: str, node_ip: str, metadata: Optional[Dict] = None):
        FSStorageCheckpoint.__init__(
            self, path=path, node_ip=node_ip, metadata=metadata
        )

    def __eq__(self, other):
        return isinstance(
            other, RemoteNodeStorageCheckpoint
        ) and FSStorageCheckpoint.__eq__(self, other)

    def to_local_storage_checkpoint(
        self, path: Optional[str] = None
    ) -> LocalStorageCheckpoint:
        remote_pack = ray.remote(_pack).options(resources=f"node:{self.node_ip}")
        packed = ray.get(remote_pack.remote(self.path))
        _unpack(packed, path)
        return LocalStorageCheckpoint(path=path, metadata=self.metadata)


class ExternalStorageCheckpoint(Checkpoint):
    def __init__(self, location: str, metadata: Optional[Dict] = None):
        Checkpoint.__init__(self, metadata=metadata)
        self.location = location

    def to_dict(self) -> dict:
        path = self.to_directory()
        local_checkpoint = Checkpoint.from_directory(path)
        data_dict = local_checkpoint.to_dict()
        shutil.rmtree(local_checkpoint.path)
        return data_dict

    def to_directory(self, path: Optional[str] = None) -> str:
        path = path if path is not None else temporary_checkpoint_dir()
        download_from_bucket(bucket=self.location, local_path=path)
        return path

    def to_data_checkpoint(self) -> "DataCheckpoint":
        data_dict = self.to_dict()
        return Checkpoint.from_dict(data_dict)

    def to_local_storage_checkpoint(
        self, path: Optional[str] = None
    ) -> "LocalStorageCheckpoint":
        path = self.to_directory()
        return Checkpoint.from_directory(path)

    def to_object_store_checkpoint(self) -> "ObjectStoreCheckpoint":
        checkpoint = self.to_data_checkpoint()
        return checkpoint.to_object_store_checkpoint()

    def to_external_storage_checkpoint(
        self, location: str
    ) -> "ExternalStorageCheckpoint":
        if location == self.location:
            return self

        raise NotImplementedError(
            "Copying from cloud to cloud is currently not supported."
        )

    def __eq__(self, other):
        return (
            isinstance(other, ExternalStorageCheckpoint)
            and Checkpoint.__eq__(self, other)
            and self.location == other.location
        )


class MultiLocationCheckpoint(Checkpoint):
    def __init__(self, *locations: Checkpoint):
        Checkpoint.__init__(self, metadata=None)
        self.locations = locations

    @property
    def path(self) -> Optional[str]:
        """Convenience function to return first local path"""
        for location in self.locations:
            if isinstance(location, FSStorageCheckpoint):
                return location.path
        return None

    def search_checkpoint(
        self, checkpoint_cls: Type[Checkpoint]
    ) -> Optional[Checkpoint]:
        for location in self.locations:
            if isinstance(location, checkpoint_cls):
                return location
        return None

    def __eq__(self, other):
        return (
            isinstance(other, MultiLocationCheckpoint)
            and Checkpoint.__eq__(self, other)
            and self.locations == other.locations
        )
