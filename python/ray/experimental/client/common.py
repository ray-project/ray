import ray.core.generated.ray_client_pb2 as ray_client_pb2
from ray.experimental.client import ray
from typing import Any
from ray import cloudpickle


class ClientObjectRef:
    def __init__(self, id):
        self.id = id

    def __repr__(self):
        return "ClientObjectRef(%s)" % self.id.hex()

    def __eq__(self, other):
        return self.id == other.id


class ClientRemoteFunc:
    def __init__(self, f):
        self._func = f
        self._name = f.__name__
        self.id = None
        self._raylet_remote_func = None

    def __call__(self, *args, **kwargs):
        raise TypeError(f"Remote function cannot be called directly. "
                        "Use {self._name}.remote method instead")

    def remote(self, *args, **kwargs):
        return ray.call_remote(self, *args, **kwargs)

    def __repr__(self):
        return "ClientRemoteFunc(%s, %s)" % (self._name, self.id)

    def set_remote_func(self, func):
        self._raylet_remote_func = func

    def run_remote_func(self, *args, **kwargs):
        self._raylet_remote_func.remote(*args, **kwargs)


def convert_from_arg(pb) -> Any:
    if pb.local == ray_client_pb2.Arg.Locality.REFERENCE:
        return ClientObjectRef(pb.reference_id)
    elif pb.local == ray_client_pb2.Arg.Locality.INTERNED:
        return cloudpickle.loads(pb.data)

    raise Exception("convert_from_arg: Uncovered locality enum")


def convert_to_arg(val):
    out = ray_client_pb2.Arg()
    if isinstance(val, ClientObjectRef):
        out.local = ray_client_pb2.Arg.Locality.REFERENCE
        out.reference_id = val.id
    else:
        out.local = ray_client_pb2.Arg.Locality.INTERNED
        out.data = cloudpickle.dumps(val)
    return out
