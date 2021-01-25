import ray
from ray.util import inspect_serializability
from ray import serve


def f(_): pass

client = serve.start()
client.create_backend("f", f)
client.create_endpoint("f", backend="f")
handle = client.get_handle("f")

inspect_serializability(handle, name="my_handle_name")

ray.put(handle)