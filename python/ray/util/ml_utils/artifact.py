import abc
import time
from typing import Any

import ray


class Artifact(abc.ABC):
    def __init__(self):
        self.creation_time = time.time()


class FutureArtifact(abc.ABC):
    def __init__(self, future: ray.ObjectRef):
        self.future = future


class ObjectStoreArtifact(Artifact):
    def __init__(self, data: Any):
        self.data = data


class LocalStorageArtifact(Artifact):
    def __init__(self, path: str):
        self.path = path
        self.node_ip = ray.util.get_node_ip_address()

    def __reduce_ex__(self, protocol):
        if self.node_ip == ray.util.get_node_ip_address():
            return LocalStorageArtifact, self.path
        else:
            return RemoteNodeStorageArtifact, self.path, self.node_ip


class RemoteNodeStorageArtifact(Artifact):
    def __init__(self, path: str, node_ip: str):
        self.path = path
        self.node_ip = str

    def __reduce_ex__(self, protocol):
        if self.node_ip == ray.util.get_node_ip_address():
            return LocalStorageArtifact, self.path
        else:
            return RemoteNodeStorageArtifact, self.path, self.node_ip


class CloudStorageArtifact(Artifact):
    def __init__(self, location: str):
        self.location = location
