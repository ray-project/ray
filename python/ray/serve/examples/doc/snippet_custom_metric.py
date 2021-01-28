import ray
from ray import serve
from ray.util import metrics

import time

ray.init(address="auto")
client = serve.start()


class MyBackendClass:
    def __init__(self):
        self.my_counter = metrics.Count(
            "my_counter",
            description=("The number of excellent requests to this backend."),
            tag_keys=("backend", ))
        self.my_counter.set_default_tags({
            "backend": serve.get_current_backend_tag()
        })

    def __call__(self, request):
        if "excellent" in request.query_params:
            self.my_counter.record(1)


client.create_backend("my_backend", MyBackendClass)
client.create_endpoint("my_endpoint", backend="my_backend")

handle = client.get_handle("my_endpoint")
while (True):
    ray.get(handle.remote(excellent=True))
    time.sleep(1)
