import cloudpickle
import io
import sys
import ray

from typing import Any

from ray.experimental.client.client_pickler import PickleStub
from ray.experimental.client.server.server_stubs import ServerFunctionSentinel


if sys.version_info < (3, 8):
    try:
        import pickle5 as pickle  # noqa: F401
    except ImportError:
        import pickle  # noqa: F401
else:
    import pickle  # noqa: F401


class ServerPickler(cloudpickle.CloudPickler):
    def __init__(self, client_id, server, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client_id = client_id
        self.server = server

    def persistent_id(self, obj):
        if isinstance(obj, ray.ObjectRef):
            return PickleStub(
                type="Object",
                client_id=self.client_id,
                ref_id=obj.binary(),
            )
        elif isinstance(obj, ray.actor.ActorHandle):
            return PickleStub(
                type="Actor",
                client_id=self.client_id,
                ref_id=obj._actor_id.binary(),
            )
        return None


class ClientUnpickler(pickle.Unpickler):
    def __init__(self, server, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server = server

    def persistent_load(self, pid):
        assert isinstance(pid, PickleStub)
        if pid.type == "Object":
            return self.server.object_refs[pid.client_id][pid.ref_id]
        elif pid.type == "Actor":
            return self.server.actor_refs[pid.ref_id]
        elif pid.type == "RemoteFunc":
            if len(pid.ref_id) == 0:
                # This is a recursive func
                return ServerFunctionSentinel()
            return self.server.lookup_or_register_func(
                pid.ref_id, pid.client_id)
        else:
            raise NotImplementedError("Uncovered client data type")


def dumps_from_server(obj, client_id, server_instance, protocol=None, buffer_callback=None):
    with io.BytesIO() as file:
        sp = ServerPickler(
            client_id,
            server_instance,
            file,
            protocol=protocol,
            buffer_callback=buffer_callback)
        sp.dump(obj)
        return file.getvalue()


def loads_from_client(data, server_instance, *,
                      fix_imports=True, encoding="ASCII", errors="strict", buffers=None):
    if isinstance(data, str):
        raise TypeError("Can't load pickle from unicode string")
    file = io.BytesIO(data)
    return ClientUnpickler(
        server_instance,
        file,
        fix_imports=fix_imports,
        buffers=buffers,
        encoding=encoding,
        errors=errors).load()


def convert_from_arg(pb, server) -> Any:
    return loads_from_client(pb.data, server)

