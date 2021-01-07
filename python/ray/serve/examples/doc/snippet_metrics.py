import ray
from ray import serve

import time

ray.init(address="auto")
client = serve.start()


def f(request):
    time.sleep(1)


client.create_backend("f", f)
client.create_endpoint("f", backend="f")

handle = client.get_handle("f")
while (True):
    ray.get(handle.remote())
