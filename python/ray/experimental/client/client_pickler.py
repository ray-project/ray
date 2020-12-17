import cloudpickle
import io
import sys

from typing import NamedTuple

from ray.experimental.client.common import ClientObjectRef
from ray.experimental.client.common import ClientActorHandle
from ray.experimental.client.common import ClientRemoteFunc
import ray.core.generated.ray_client_pb2 as ray_client_pb2

if sys.version_info < (3, 8):
    try:
        import pickle5 as pickle  # noqa: F401
    except ImportError:
        import pickle  # noqa: F401
else:
    import pickle  # noqa: F401


PickleStub = NamedTuple(
    "PickleStub",
    [("type", str), ("client_id", str), ("ref_id", bytes)])


class ClientPickler(cloudpickle.CloudPickler):
    def __init__(self, client_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client_id = client_id

    def persistent_id(self, obj):
        if isinstance(obj, ClientObjectRef):
            return PickleStub(
                type="Object",
                client_id=self.client_id,
                ref_id=obj.id,
            )
        elif isinstance(obj, ClientActorHandle):
            return PickleStub(
                type="Actor",
                client_id=self.client_id,
                ref_id=obj._actor_id,
            )
        elif isinstance(obj, ClientRemoteFunc):
            if obj._ref is None:
                obj._ensure_ref()
            return PickleStub(
                type="RemoteFunc",
                client_id=self.client_id,
                ref_id=obj._ref.id
            )
        return None


class ServerUnpickler(pickle.Unpickler):
    def persistent_load(self, pid):
        assert isinstance(pid, PickleStub)
        if pid.type == "Object":
            return ClientObjectRef(id=pid.ref_id)
        elif pid.type == "Actor":
            return ClientActorHandle(ClientActorRef(id=pid.ref_id))
        else:
            raise NotImplementedError("Being passed back an unknown stub")


def dumps_from_client(obj, client_id, protocol=None, buffer_callback=None):
    with io.BytesIO() as file:
        cp = ClientPickler(
            client_id, file, protocol=protocol, buffer_callback=buffer_callback
        )
        cp.dump(obj)
        return file.getvalue()


def loads_from_server(data, *, fix_imports=True, encoding="ASCII", errors="strict", buffers=None):
    if isinstance(data, str):
        raise TypeError("Can't load pickle from unicode string")
    file = io.BytesIO(data)
    return ServerUnpickler(
        file,
        fix_imports=fix_imports,
        buffers=buffers,
        encoding=encoding,
        errors=errors).load()


def convert_to_arg(val, client_id):
    out = ray_client_pb2.Arg()
    out.local = ray_client_pb2.Arg.Locality.INTERNED
    out.data = dumps_from_client(val, client_id)
    return out
