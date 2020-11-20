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


class ClientRemoteActor:
    def __init__(self, actor_cls):
        self.actor_cls = actor_cls
        self._name = actor_cls.__name__
        self.id = None
        self._raylet_remote_actor = None

    def __call__(self, *args, **kwargs):
        raise TypeError(f"Remote actor cannot be instantiated directly. "
                        "Use {self._name}.remote() instead")

    def remote(self, *args, **kwargs):
        pass

    def __repr__(self):
        return "ClientRemoteActor(%s, %s)" % (self._name, self.id)

    def _set_remote_actor(self, actor):
        self._raylet_remote_actor = actor

    def _run_remote_func(self, name, *args, **kwargs):
        getattr(self._raylet_remote_func, name).remote(*args, **kwargs)

    def __getattr__(self, key):
        return ClientRemoteMethod(self, method_name)


class ClientRemoteMethod:
    def __init__(self, remote_actor, method_name):
        self.remote_actor = remote_actor
        self.method_name = method_name
        self._name = "%s.%s" % (
            self.remote_actor._name, self.method_name, self.id)

    def __call__(self, *args, **kwargs):
        raise TypeError(f"Remote method cannot be called directly. "
                        "Use {self._name}.remote() instead")

    def remote(self, *args, **kwargs):
        pass

    def __repr__(self):
        return "ClientRemoteMethod(%s, %s)" % (self._name, self.id)


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
