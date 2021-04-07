"""Implements the client side of the client/server pickling protocol.

All ray client client/server data transfer happens through this pickling
protocol. The model is as follows:

    * All Client objects (eg ClientObjectRef) always live on the client and
      are never represented in the server
    * All Ray objects (eg, ray.ObjectRef) always live on the server and are
      never returned to the client
    * In order to translate between these two references, PickleStub tuples
      are generated as persistent ids in the data blobs during the pickling
      and unpickling of these objects.

The PickleStubs have just enough information to find or generate their
associated partner object on either side.

This also has the advantage of avoiding predefined pickle behavior for ray
objects, which may include ray internal reference counting.

ClientPickler dumps things from the client into the appropriate stubs
ServerUnpickler loads stubs from the server into their client counterparts.
"""

import io
import sys

from typing import NamedTuple
from typing import Any
from typing import Dict
from typing import Optional

import ray.cloudpickle as cloudpickle
from ray.util.client import RayAPIStub
from ray.util.client.common import ClientObjectRef
from ray.util.client.common import ClientActorHandle
from ray.util.client.common import ClientActorRef
from ray.util.client.common import ClientActorClass
from ray.util.client.common import ClientRemoteFunc
from ray.util.client.common import ClientRemoteMethod
from ray.util.client.common import OptionWrapper
from ray.util.client.common import InProgressSentinel
import ray.core.generated.ray_client_pb2 as ray_client_pb2
from ray._private.client_mode_hook import disable_client_hook

if sys.version_info < (3, 8):
    try:
        import pickle5 as pickle  # noqa: F401
    except ImportError:
        import pickle  # noqa: F401
else:
    import pickle  # noqa: F401

# NOTE(barakmich): These PickleStubs are really close to
# the data for an exectuion, with no arguments. Combine the two?
PickleStub = NamedTuple("PickleStub",
                        [("type", str), ("client_id", str), ("ref_id", bytes),
                         ("name", Optional[str]),
                         ("baseline_options", Optional[Dict])])


class ClientPickler(cloudpickle.CloudPickler):
    def __init__(self, client_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client_id = client_id

    def persistent_id(self, obj):
        if isinstance(obj, RayAPIStub):
            return PickleStub(
                type="Ray",
                client_id=self.client_id,
                ref_id=b"",
                name=None,
                baseline_options=None,
            )
        elif isinstance(obj, ClientObjectRef):
            return PickleStub(
                type="Object",
                client_id=self.client_id,
                ref_id=obj.id,
                name=None,
                baseline_options=None,
            )
        elif isinstance(obj, ClientActorHandle):
            return PickleStub(
                type="Actor",
                client_id=self.client_id,
                ref_id=obj._actor_id,
                name=None,
                baseline_options=None,
            )
        elif isinstance(obj, ClientRemoteFunc):
            if obj._ref is None:
                obj._ensure_ref()
            if type(obj._ref) == InProgressSentinel:
                return PickleStub(
                    type="RemoteFuncSelfReference",
                    client_id=self.client_id,
                    ref_id=obj._client_side_ref.id,
                    name=None,
                    baseline_options=None,
                )
            return PickleStub(
                type="RemoteFunc",
                client_id=self.client_id,
                ref_id=obj._ref.id,
                name=None,
                baseline_options=obj._options,
            )
        elif isinstance(obj, ClientActorClass):
            if obj._ref is None:
                obj._ensure_ref()
            if type(obj._ref) == InProgressSentinel:
                return PickleStub(
                    type="RemoteActorSelfReference",
                    client_id=self.client_id,
                    ref_id=obj._client_side_ref.id,
                    name=None,
                    baseline_options=None,
                )
            return PickleStub(
                type="RemoteActor",
                client_id=self.client_id,
                ref_id=obj._ref.id,
                name=None,
                baseline_options=obj._options,
            )
        elif isinstance(obj, ClientRemoteMethod):
            return PickleStub(
                type="RemoteMethod",
                client_id=self.client_id,
                ref_id=obj.actor_handle.actor_ref.id,
                name=obj.method_name,
                baseline_options=None,
            )
        elif isinstance(obj, OptionWrapper):
            raise NotImplementedError(
                "Sending a partial option is unimplemented")
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


def dumps_from_client(obj: Any, client_id: str, protocol=None) -> bytes:
    with disable_client_hook():
        with io.BytesIO() as file:
            cp = ClientPickler(client_id, file, protocol=protocol)
            cp.dump(obj)
            return file.getvalue()


def loads_from_server(data: bytes,
                      *,
                      fix_imports=True,
                      encoding="ASCII",
                      errors="strict") -> Any:
    if isinstance(data, str):
        raise TypeError("Can't load pickle from unicode string")
    file = io.BytesIO(data)
    return ServerUnpickler(
        file, fix_imports=fix_imports, encoding=encoding,
        errors=errors).load()


def convert_to_arg(val: Any, client_id: str) -> ray_client_pb2.Arg:
    out = ray_client_pb2.Arg()
    out.local = ray_client_pb2.Arg.Locality.INTERNED
    out.data = dumps_from_client(val, client_id)
    return out
