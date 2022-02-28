import abc
import os
import tarfile
import tempfile
import time
from typing import Any, Optional

import ray


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


class Artifact(abc.ABC):
    def __init__(self):
        self.creation_time = time.time()


class DataArtifact(Artifact):
    def __init__(self, data: Any):
        Artifact.__init__(self)
        self.data = data

    def __eq__(self, other):
        return isinstance(other, DataArtifact) and self.data == other.data


class ObjectStoreArtifact(Artifact):
    def __init__(self, obj_ref: ray.ObjectRef):
        Artifact.__init__(self)
        self.obj_ref = obj_ref

    def __eq__(self, other):
        return isinstance(other, ObjectStoreArtifact) and self.obj_ref == other.obj_ref


class FSStorageArtifact(Artifact, abc.ABC):
    def __init__(self, path: str, node_ip: str):
        Artifact.__init__(self)
        self.path = path
        self.node_ip = node_ip

    def __reduce_ex__(self, protocol):
        if self.node_ip == ray.util.get_node_ip_address():
            return LocalStorageArtifact, self.path
        else:
            return RemoteNodeStorageArtifact, self.path, self.node_ip

    def __eq__(self, other):
        return (
            isinstance(other, FSStorageArtifact)
            and self.path == other.path
            and self.node_ip == other.node_ip
        )


class LocalStorageArtifact(FSStorageArtifact):
    def __init__(self, path: str):
        FSStorageArtifact.__init__(
            self, path=path, node_ip=ray.util.get_node_ip_address()
        )

    def __eq__(self, other):
        return isinstance(other, LocalStorageArtifact) and FSStorageArtifact.__eq__(
            self, other
        )


class RemoteNodeStorageArtifact(FSStorageArtifact):
    def __init__(self, path: str, node_ip: str):
        FSStorageArtifact.__init__(self, path=path, node_ip=node_ip)
        self.path = path
        self.node_ip = node_ip

    def __eq__(self, other):
        return isinstance(
            other, RemoteNodeStorageArtifact
        ) and FSStorageArtifact.__eq__(self, other)

    def to_local_storage_artifact(self, path: str) -> "LocalStorageArtifact":
        remote_pack = ray.remote(_pack).options(resources=f"node:{self.node_ip}")
        packed = ray.get(remote_pack.remote(self.path))
        _unpack(packed, path)
        return LocalStorageArtifact(path=path)


class CloudStorageArtifact(Artifact):
    def __init__(self, location: str):
        Artifact.__init__(self)
        self.location = location

    def __eq__(self, other):
        return (
            isinstance(other, CloudStorageArtifact) and self.location == other.location
        )


class MultiLocationArtifact(Artifact):
    def __init__(self, *locations: Artifact):
        Artifact.__init__(self)
        self.locations = locations

    @property
    def path(self) -> Optional[str]:
        """Convenience function to return first local path"""
        for location in self.locations:
            if isinstance(location, FSStorageArtifact):
                return location.path
        return None

    def __eq__(self, other):
        return (
            isinstance(other, MultiLocationArtifact)
            and self.locations == other.locations
        )
