import cloudpickle
import io
import sys

from typing import Any

from ray.experimental.client.common import ClientObjectRef
import ray.core.generated.ray_client_pb2 as ray_client_pb2

if sys.version_info < (3, 8):
    try:
        import pickle5 as pickle  # noqa: F401
    except ImportError:
        import pickle  # noqa: F401
else:
    import pickle  # noqa: F401


class ClientPickler(cloudpickle.CloudPickler):
    def __init__(self, client_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client_id = client_id

    def persistent_id(self, obj):
        if isinstance(obj, ClientObjectRef):
            return ("Object", self.client_id, obj.id)
        return None


class ClientUnpickler(pickle.Unpickler):
    def __init__(self, server, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server = server

    def persistent_load(self, pid):
        t, client_id, obj_id = pid
        assert t == "Object"
        return self.server.object_refs[client_id][obj_id]


def dumps(obj, client_id, protocol=None, buffer_callback=None):
    """Serialize obj as a string of bytes allocated in memory

    protocol defaults to cloudpickle.DEFAULT_PROTOCOL which is an alias to
    pickle.HIGHEST_PROTOCOL. This setting favors maximum communication
    speed between processes running the same Python version.

    Set protocol=pickle.DEFAULT_PROTOCOL instead if you need to ensure
    compatibility with older versions of Python.
    """
    with io.BytesIO() as file:
        cp = ClientPickler(
            client_id, file, protocol=protocol, buffer_callback=buffer_callback
        )
        cp.dump(obj)
        return file.getvalue()


def loads(data, server_instance, *,
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
    return loads(pb.data, server)


def convert_to_arg(val, client_id):
    out = ray_client_pb2.Arg()
    out.local = ray_client_pb2.Arg.Locality.INTERNED
    out.data = dumps(val, client_id)
    return out
