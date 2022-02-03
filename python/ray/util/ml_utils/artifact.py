import abc
import time
from typing import Any

import ray


class Artifact(abc.ABC):
    def __init__(self):
        self.creation_time = time.time()


class DataArtifact(Artifact):
    def __init__(self, data: Any):
        super().__init__()
        self.data = data


class ObjectStoreArtifact(Artifact):
    def __init__(self, obj_ref: ray.ObjectRef):
        super().__init__()
        self.obj_ref = obj_ref


class FSStorageArtifact(Artifact, abc.ABC):
    def __init__(self, path: str, node_ip: str):
        super().__init__()
        self.path = path
        self.node_ip = node_ip

    def __reduce_ex__(self, protocol):
        if self.node_ip == ray.util.get_node_ip_address():
            return LocalStorageArtifact, self.path
        else:
            return RemoteNodeStorageArtifact, self.path, self.node_ip


class LocalStorageArtifact(FSStorageArtifact):
    def __init__(self, path: str):
        super().__init__(path=path, node_ip=ray.util.get_node_ip_address())


class RemoteNodeStorageArtifact(FSStorageArtifact):
    def __init__(self, path: str, node_ip: str):
        super().__init__(path=path, node_ip=node_ip)
        self.path = path
        self.node_ip = node_ip


class CloudStorageArtifact(Artifact):
    def __init__(self, location: str):
        super().__init__()
        self.location = location


class MultiLocationArtifact(Artifact):
    def __init__(self, *locations: Artifact):
        super().__init__()
        self.locations = locations
