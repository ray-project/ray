import abc
import json
import shutil
import tempfile

import cloudpickle as pickle
import os
from typing import Any, Type, Optional, Dict

import ray
from ray.ml.checkpoint import Checkpoint
from ray.util.ml_utils.cloud import (
    _upload_to_bucket,
    is_cloud_target,
    _download_from_bucket,
)
from ray.util.ml_utils.artifact import (
    Artifact,
    LocalStorageArtifact,
    ObjectStoreArtifact,
    RemoteNodeStorageArtifact,
    CloudStorageArtifact,
    MultiLocationArtifact,
    FSStorageArtifact,
    DataArtifact,
    _pack,
    _unpack,
)


def write_metadata(path: str, metadata: Dict[str, Any]) -> None:
    metadata_file = os.path.join(path, ".tune_metadata")
    with open(metadata_file, "wt") as fp:
        json.dump(metadata, fp)


def load_metadata(path: str) -> Dict[str, Any]:
    metadata_file = os.path.join(path, ".tune_metadata")
    if os.path.exists(metadata_file):
        with open(metadata_file, "rt") as fp:
            return json.load(fp)


def temporary_checkpoint_dir() -> str:
    return tempfile.mkdtemp(prefix="checkpoint_tmp_", dir=os.getcwd())


class TuneCheckpoint(Checkpoint, Artifact, abc.ABC):
    """Checkpoint implementation used in Ray Tune runs.

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

    def __init__(self, metadata: Any = None):
        Checkpoint.__init__(self, metadata=metadata)
        Artifact.__init__(self)

    def __eq__(self, other):
        return isinstance(other, TuneCheckpoint) and self.metadata == other.metadata

    def to_bytes(self) -> bytes:
        return pickle.dumps(self.to_dict())

    @classmethod
    def from_bytes(cls, data: bytes) -> "DataCheckpoint":
        bytes_data = pickle.loads(data)
        return cls.from_dict(bytes_data)

    def to_dict(self) -> dict:
        raise NotImplementedError

    @classmethod
    def from_dict(cls, data: dict) -> "DataCheckpoint":
        metadata = data.get("metadata", {})
        if metadata.get("is_data_checkpoint", False):
            checkpoint_data = data["data"]
        else:
            checkpoint_data = data
        return DataCheckpoint(data=checkpoint_data, metadata=metadata)

    def to_directory(self, path: Optional[str] = None) -> str:
        raise NotImplementedError

    @classmethod
    def from_directory(cls, path: str) -> "LocalStorageCheckpoint":
        metadata = load_metadata(path)
        local_checkpoint = LocalStorageCheckpoint(path=path, metadata=metadata)
        return local_checkpoint

    def to_object_ref(self) -> ray.ObjectRef:
        return ray.put(self.to_dict())

    @classmethod
    def from_object_ref(cls, obj_ref: ray.ObjectRef) -> "ObjectStoreCheckpoint":
        return ObjectStoreCheckpoint(obj_ref=obj_ref, metadata=None)

    def to_uri(self, location: str) -> str:
        path = self.to_directory(path=None)
        assert is_cloud_target(location)
        _upload_to_bucket(bucket=location, local_path=path)
        if not isinstance(self, FSStorageCheckpoint):
            # Only delete directory if it was a temporary directory
            shutil.rmtree(path)
        return location

    @classmethod
    def from_uri(cls, location: str) -> "CloudStorageCheckpoint":
        return CloudStorageCheckpoint(location=location)

    def to_data_checkpoint(self) -> "DataCheckpoint":
        raise NotImplementedError

    def to_object_store_checkpoint(self) -> "ObjectStoreCheckpoint":
        raise NotImplementedError

    def to_local_storage_checkpoint(
        self, path: Optional[str] = None
    ) -> "LocalStorageCheckpoint":
        raise NotImplementedError

    def to_cloud_storage_checkpoint(self, location: str) -> "CloudStorageCheckpoint":
        location = self.to_uri(location=location)
        return CloudStorageCheckpoint(location=location)


class DataCheckpoint(TuneCheckpoint, DataArtifact):
    def __init__(self, data: Any, metadata: Any = None):
        TuneCheckpoint.__init__(self, metadata=metadata)
        DataArtifact.__init__(self, data=data)

    @property
    def is_fs_checkpoint(self):
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
            and DataArtifact.__eq__(self, other)
            and TuneCheckpoint.__eq__(self, other)
        )


class ObjectStoreCheckpoint(TuneCheckpoint, ObjectStoreArtifact):
    def __init__(self, obj_ref: ray.ObjectRef, metadata: Any):
        TuneCheckpoint.__init__(self, metadata=metadata)
        ObjectStoreArtifact.__init__(self, obj_ref=obj_ref)

    def to_dict(self) -> dict:
        data = ray.get(self.obj_ref)
        return TuneCheckpoint.from_dict(data).to_dict()

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
            and ObjectStoreArtifact.__eq__(self, other)
            and TuneCheckpoint.__eq__(self, other)
        )


class FSStorageCheckpoint(TuneCheckpoint, FSStorageArtifact, abc.ABC):
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
            and FSStorageArtifact.__eq__(self, other)
            and TuneCheckpoint.__eq__(self, other)
        )

    def __repr__(self):
        return f"<{self.__class__.__name__} path={self.path} node_ip={self.node_ip}>"


class LocalStorageCheckpoint(FSStorageCheckpoint, LocalStorageArtifact):
    def __init__(self, path: str, metadata: Any = None):
        TuneCheckpoint.__init__(self, metadata=metadata)
        LocalStorageArtifact.__init__(self, path=path)

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
        return (
            isinstance(other, LocalStorageCheckpoint)
            and LocalStorageArtifact.__eq__(self, other)
            and FSStorageCheckpoint.__eq__(self, other)
        )


class RemoteNodeStorageCheckpoint(FSStorageCheckpoint, RemoteNodeStorageArtifact):
    def __init__(self, path: str, node_ip: str, metadata: Any = None):
        TuneCheckpoint.__init__(self, metadata=metadata)
        RemoteNodeStorageArtifact.__init__(self, path=path, node_ip=node_ip)

    def __eq__(self, other):
        return (
            isinstance(other, RemoteNodeStorageCheckpoint)
            and RemoteNodeStorageArtifact.__eq__(self, other)
            and FSStorageCheckpoint.__eq__(self, other)
        )

    def to_local_storage_checkpoint(
        self, path: Optional[str] = None
    ) -> LocalStorageCheckpoint:
        local_artifact = RemoteNodeStorageArtifact.to_local_storage_artifact(self, path)
        return LocalStorageCheckpoint(path=local_artifact.path, metadata=self.metadata)


class CloudStorageCheckpoint(TuneCheckpoint, CloudStorageArtifact):
    def __init__(self, location: str, metadata: Any = None):
        TuneCheckpoint.__init__(self, metadata=metadata)
        CloudStorageArtifact.__init__(self, location=location)

    def to_dict(self) -> dict:
        path = self.to_directory()
        local_checkpoint = TuneCheckpoint.from_directory(path)
        data_dict = local_checkpoint.to_dict()
        shutil.rmtree(local_checkpoint.path)
        return data_dict

    def to_directory(self, path: Optional[str] = None) -> str:
        path = path if path is not None else temporary_checkpoint_dir()
        _download_from_bucket(bucket=self.location, local_path=path)
        return path

    def to_data_checkpoint(self) -> "DataCheckpoint":
        data_dict = self.to_dict()
        return TuneCheckpoint.from_dict(data_dict)

    def to_local_storage_checkpoint(
        self, path: Optional[str] = None
    ) -> "LocalStorageCheckpoint":
        path = self.to_directory()
        return TuneCheckpoint.from_directory(path)

    def to_object_store_checkpoint(self) -> "ObjectStoreCheckpoint":
        checkpoint = self.to_data_checkpoint()
        return checkpoint.to_object_store_checkpoint()

    def to_cloud_storage_checkpoint(self, location: str) -> "CloudStorageCheckpoint":
        if location == self.location:
            return self

        raise NotImplementedError(
            "Copying from cloud to cloud is currently not supported."
        )

    def __eq__(self, other):
        return (
            isinstance(other, CloudStorageCheckpoint)
            and CloudStorageArtifact.__eq__(self, other)
            and TuneCheckpoint.__eq__(self, other)
        )


class MultiLocationCheckpoint(TuneCheckpoint, MultiLocationArtifact):
    def __init__(self, *locations: TuneCheckpoint):
        TuneCheckpoint.__init__(self, metadata=None)
        MultiLocationArtifact.__init__(self, *locations)

    def search_checkpoint(
        self, checkpoint_cls: Type[TuneCheckpoint]
    ) -> Optional[TuneCheckpoint]:
        for location in self.locations:
            if isinstance(location, checkpoint_cls):
                return location
        return None

    def __eq__(self, other):
        return (
            isinstance(other, MultiLocationCheckpoint)
            and MultiLocationArtifact.__eq__(self, other)
            and TuneCheckpoint.__eq__(self, other)
        )
