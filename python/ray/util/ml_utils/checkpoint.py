import abc
import cloudpickle as pickle
import os
from typing import Any

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


class CheckpointInterface(abc.ABC):
    def load_model(self) -> "Model":
        raise NotImplementedError

    def load_preprocessor(self) -> "Preprocessor":
        raise NotImplementedError


class Checkpoint(Artifact, abc.ABC):
    def __init__(self, metadata: Any = None):
        super().__init__()
        self.metadata = metadata


class DataCheckpoint(Checkpoint, DataArtifact):
    def __init__(self, data: Any, metadata: Any):
        Checkpoint.__init__(self, metadata=metadata)
        DataArtifact.__init__(self, data=data)

    def to_local_storage(self, path: str):
        """Convert DataCheckpoint to LocalStorageCheckpoint"""
        os.makedirs(path, exist_ok=True)
        # Drop marker
        open(os.path.join(path, ".is_checkpoint"), "a").close()
        # Dump into checkpoint.pkl
        checkpoint_data_path = os.path.join(path, "checkpoint.pkl")
        with open(checkpoint_data_path, "wb") as f:
            pickle.dump(self.data, f)
        new_metadata = self.metadata.copy()
        new_metadata["is_data_checkpoint"] = True
        local_checkpoint = LocalStorageCheckpoint(
            path=checkpoint_data_path, metadata=new_metadata
        )
        local_checkpoint.write_metadata()


class ObjectStoreCheckpoint(Checkpoint, ObjectStoreArtifact):
    def __init__(self, obj_ref: ray.ObjectRef, metadata: Any):
        Checkpoint.__init__(self, metadata=metadata)
        ObjectStoreArtifact.__init__(self, obj_ref=obj_ref)


class FSStorageCheckpoint(Checkpoint, FSStorageArtifact, abc.ABC):
    @property
    def is_data_checkpoint(self) -> bool:
        """Return True if this can be converted back to data checkpoint."""
        return self.metadata.get("is_data_checkpoint", False)

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
