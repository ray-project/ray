import ray
from ray import serve

import time

ray.init(address="auto")
serve.start()


def f(request):
    time.sleep(1)


serve.create_backend("f", f)
serve.create_endpoint("f", backend="f")

handle = serve.get_handle("f")
while (True):
    ray.get(handle.remote())
