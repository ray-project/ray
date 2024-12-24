"""Implements Pickling for client2.

Idea is from the original Ray Client: special treatment to the ObjectRef, but much
simpler.

On the server side: real ray.ObjectRef, with references kept in the server.
On the fly: PickledObjectRef which is just bytes
On the client side: still ray.ObjectRef, but without any references.
"""
import sys
import ray
import io

from typing import Any, NamedTuple
import ray.cloudpickle as cloudpickle


if sys.version_info < (3, 8):
    try:
        import pickle5 as pickle  # noqa: F401
    except ImportError:
        import pickle  # noqa: F401
else:
    import pickle  # noqa: F401


# Represents an ObjectRef.
class PickledObjectRef(NamedTuple("PickledObjectRef", [("ref_id", bytes)])):
    def __reduce__(self):
        # PySpark's namedtuple monkey patch breaks compatibility with
        # cloudpickle. Thus we revert this patch here if it exists.
        return object.__reduce__(self)


class ServerToClientPickler(cloudpickle.CloudPickler):
    def __init__(self, server, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server = server

    def persistent_id(self, obj):
        if isinstance(obj, ray.ObjectRef):
            obj_id = obj.binary()
            if obj_id not in self.server.object_refs:
                # We're passing back a reference, probably inside a reference.
                # Let's hold onto it.
                self.server.object_refs[obj_id] = obj
            return PickledObjectRef(ref_id=obj_id)
        return None


class ServerToClientUnpickler(pickle.Unpickler):
    def persistent_load(self, pid):
        if isinstance(pid, PickledObjectRef):
            return ray.ObjectRef(pid.ref_id)
        raise pickle.UnpicklingError("unknown type")


class ClientToServerPickler(cloudpickle.CloudPickler):
    # TODO: "ray" and more?
    def persistent_id(self, obj):
        if isinstance(obj, ray.ObjectRef):
            obj_id = obj.binary()
            return PickledObjectRef(ref_id=obj_id)
        return None


class ClientToServerUnpickler(pickle.Unpickler):
    def __init__(self, server, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.server = server

    def persistent_load(self, pid):
        if isinstance(pid, PickledObjectRef):
            return self.server.object_refs[pid.ref_id]
        raise pickle.UnpicklingError("unknown type")


def dumps_with_pickler_cls(cls, *args, **kwargs):
    # Usage:
    # my_dumps = dumps_with_pickler_cls(ServerToClientPickler, server)
    # bytes = my_dumps(value)
    def dumps(obj: Any):
        with io.BytesIO() as file:
            pickler = cls(file=file, *args, **kwargs)
            pickler.dump(obj)
            return file.getvalue()

    return dumps


def loads_with_unpickler_cls(cls, *args, **kwargs):
    # Usage:
    # my_loads = loads_with_unpickler_cls(ClientToServerUnpickler, server)
    # value = my_loads(bytes)
    def loads(data: bytes):
        file = io.BytesIO(data)
        unpickler = cls(file=file, *args, **kwargs)
        return unpickler.load()

    return loads
