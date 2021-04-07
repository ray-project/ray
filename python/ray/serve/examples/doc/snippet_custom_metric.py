import ray
from ray import serve
from ray.util import metrics

import time

ray.init(address="auto")
serve.start()


class MyBackendClass:
    def __init__(self):
        self.my_counter = metrics.Counter(
            "my_counter",
            description=("The number of excellent requests to this backend."),
            tag_keys=("backend", ))
        self.my_counter.set_default_tags({
            "backend": serve.get_current_backend_tag()
        })

    def __call__(self, request):
        if "excellent" in request.query_params:
            self.my_counter.inc()


serve.create_backend("my_backend", MyBackendClass)
serve.create_endpoint("my_endpoint", backend="my_backend")

handle = serve.get_handle("my_endpoint")
while (True):
    ray.get(handle.remote(excellent=True))
    time.sleep(1)
