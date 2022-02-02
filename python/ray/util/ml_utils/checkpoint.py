import abc
import time

import ray
from ray.util.ml_utils.artifact import (Artifact, LocalStorageArtifact,
                                        FutureArtifact, ObjectStoreArtifact,
                                        RemoteNodeStorageArtifact,
                                        CloudStorageArtifact)


class CheckpointInterface(abc.ABC):
    def load_model(self) -> "Model":
        raise NotImplementedError

    def load_preprocessor(self) -> "Preprocessor":
        raise NotImplementedError


class Checkpoint(Artifact, abc.ABC):
    pass


class FutureCheckpoint(Checkpoint, FutureArtifact):
    pass


class ObjectStoreCheckpoint(Checkpoint, ObjectStoreArtifact):
    pass


class LocalStorageCheckpoint(Checkpoint, LocalStorageArtifact):
    def __init__(self, path: str):
        Checkpoint.__init__(self)
        LocalStorageArtifact.__init__(self, path=path)

    def __reduce_ex__(self, protocol):
        if self.node_ip == ray.util.get_node_ip_address():
            return LocalStorageCheckpoint, (self.path, )
        else:
            return RemoteNodeStorageCheckpoint, (self.path, self.node_ip)


class RemoteNodeStorageCheckpoint(Checkpoint, RemoteNodeStorageArtifact):
    def __init__(self, path: str, node_ip: str):
        Checkpoint.__init__(self)
        RemoteNodeStorageArtifact.__init__(self, path=path, node_ip=node_ip)

    def __reduce_ex__(self, protocol):
        if self.node_ip == ray.util.get_node_ip_address():
            return LocalStorageCheckpoint, (self.path, )
        else:
            return RemoteNodeStorageCheckpoint, (self.path, self.node_ip)


class CloudStorageCheckpoint(Checkpoint, CloudStorageArtifact):
    def __init__(self, location: str):
        Checkpoint.__init__(self)
        CloudStorageArtifact.__init__(self, location=location)


ca