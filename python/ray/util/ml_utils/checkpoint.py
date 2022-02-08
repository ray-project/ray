import abc
import tarfile
import tempfile

import cloudpickle as pickle
import os
from typing import Any, Type, Optional

import ray
from ray.util.ml_utils.artifact import (
    Artifact,
    LocalStorageArtifact,
    ObjectStoreArtifact,
    RemoteNodeStorageArtifact,
    CloudStorageArtifact,
    MultiLocationArtifact,
    FSStorageArtifact,
    DataArtifact,
)


def _pack(path: str) -> bytes:
    _, tmpfile = tempfile.mkstemp()
    with tarfile.open(tmpfile, "w:gz") as tar:
        tar.add(path, arcname="")

    with open(tmpfile, "rb") as f:
        stream = f.read()

    return stream


def _unpack(stream: bytes, path: str) -> str:
    _, tmpfile = tempfile.mkstemp()

    with open(tmpfile, "wb") as f:
        f.write(stream)

    with tarfile.open(tmpfile) as tar:
        tar.extractall(path)

    return path


# class CheckpointInterface(abc.ABC):
#     def load_model(self) -> "Model":
#         raise NotImplementedError
#
#     def load_preprocessor(self) -> "Preprocessor":
#         raise NotImplementedError


class Checkpoint(Artifact, abc.ABC):
    def __init__(self, metadata: Any = None):
        super().__init__()
        self.metadata = metadata


class DataCheckpoint(Checkpoint, DataArtifact):
    def __init__(self, data: Any, metadata: Any):
        Checkpoint.__init__(self, metadata=metadata)
        DataArtifact.__init__(self, data=data)

    @property
    def is_fs_checkpoint(self):
        return self.metadata.get("is_fs_checkpoint", False)

    def to_local_storage(self, path: str) -> "LocalStorageCheckpoint":
        """Convert DataCheckpoint to LocalStorageCheckpoint"""
        new_metadata = self.metadata.copy()

        os.makedirs(path, exist_ok=True)
        # Drop marker
        open(os.path.join(path, ".is_checkpoint"), "a").close()
        if self.is_fs_checkpoint:
            # Recover FS checkpoint from data
            _unpack(self.data, path)
            new_metadata["is_data_checkpoint"] = False
        else:
            # Dump into checkpoint.pkl
            checkpoint_data_path = os.path.join(path, "checkpoint.pkl")
            with open(checkpoint_data_path, "wb") as f:
                pickle.dump(self.data, f)
            new_metadata["is_data_checkpoint"] = True

        local_checkpoint = LocalStorageCheckpoint(path=path, metadata=new_metadata)
        local_checkpoint.write_metadata()
        return local_checkpoint


class ObjectStoreCheckpoint(Checkpoint, ObjectStoreArtifact):
    def __init__(self, obj_ref: ray.ObjectRef, metadata: Any):
        Checkpoint.__init__(self, metadata=metadata)
        ObjectStoreArtifact.__init__(self, obj_ref=obj_ref)


class FSStorageCheckpoint(Checkpoint, FSStorageArtifact, abc.ABC):
    @property
    def is_data_checkpoint(self) -> bool:
        """Return True if this can be converted back to data checkpoint."""
        return self.metadata.get("is_data_checkpoint", False)

    def to_data(self) -> "DataCheckpoint":
        """Convert FSStorageCheckpoint to DataCheckpoint"""
        new_metadata = self.metadata.copy()

        if self.is_data_checkpoint:
            # Restore previous DataCheckpoint from disk
            checkpoint_data_path = os.path.join(self.path, "checkpoint.pkl")
            with open(checkpoint_data_path, "wb") as f:
                data = pickle.load(f)
            new_metadata["is_fs_checkpoint"] = False
        else:
            # Pickle whole directory
            data = _pack(self.path)
            new_metadata["is_fs_checkpoint"] = True

        data_checkpoint = DataCheckpoint(data=data, metadata=new_metadata)
        return data_checkpoint

    def __reduce_ex__(self, protocol):
        if self.node_ip == ray.util.get_node_ip_address():
            return LocalStorageCheckpoint, (self.path, self.metadata)
        else:
            return RemoteNodeStorageCheckpoint, (self.path, self.node_ip, self.metadata)


class LocalStorageCheckpoint(FSStorageCheckpoint, LocalStorageArtifact):
    def __init__(self, path: str, metadata: Any = None):
        Checkpoint.__init__(self, metadata=metadata)
        LocalStorageArtifact.__init__(self, path=path)

    def write_metadata(self):
        pass


class RemoteNodeStorageCheckpoint(FSStorageCheckpoint, RemoteNodeStorageArtifact):
    def __init__(self, path: str, node_ip: str, metadata: Any = None):
        Checkpoint.__init__(self, metadata=metadata)
        RemoteNodeStorageArtifact.__init__(self, path=path, node_ip=node_ip)


class CloudStorageCheckpoint(Checkpoint, CloudStorageArtifact):
    def __init__(self, location: str, metadata: Any = None):
        Checkpoint.__init__(self, metadata=metadata)
        CloudStorageArtifact.__init__(self, location=location)


class MultiLocationCheckpoint(Checkpoint, MultiLocationArtifact):
    def __init__(self, *locations: Checkpoint):
        Checkpoint.__init__(self, metadata=None)
        MultiLocationArtifact.__init__(self, *locations)

    def search_checkpoint(
        self, checkpoint_cls: Type[Checkpoint]
    ) -> Optional[Checkpoint]:
        for location in self.locations:
            if isinstance(location, checkpoint_cls):
                return location
        return None
