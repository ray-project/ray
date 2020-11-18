from ray import cloudpickle
from ray.experimental.client.worker import ClientObjectRef

import ray
import ray.core.generated.ray_client_pb2 as ray_client_pb2


def dump_args_proto(arg):
    if arg.local == ray_client_pb2.Arg.Locality.INTERNED:
        return cloudpickle.loads(arg.data)
    else:
        # TODO(barakmich): This is a dirty hack that assumes the
        # server maintains a reference to the ID we've been given
        ref = ray.ObjectRef(arg.reference_id)
        return ray.get(ref)


def load_args_proto(thing):
    arg = ray_client_pb2.Arg()
    if isinstance(thing, ClientObjectRef):
        arg.local = ray_client_pb2.Arg.Locality.REFERENCE
        arg.reference_id = thing.id
    else:
        arg.local = ray_client_pb2.Arg.Locality.INTERNED
        arg.data = cloudpickle.dumps(thing)
    return arg
