import ray
from ray import serve
from ray.util import metrics

import time

ray.init(address="auto")
serve.start()


@serve.deployment
class MyBackend:
    def __init__(self):
        self.my_counter = metrics.Counter(
            "my_counter",
            description=("The number of excellent requests to this backend."),
            tag_keys=("backend", ))
        self.my_counter.set_default_tags({
            "backend": serve.get_current_backend_tag()
        })

    def call(self, excellent=False):
        if excellent:
            self.my_counter.inc()


MyBackend.deploy()

handle = MyBackend.get_handle()
while True:
    ray.get(handle.call.remote(excellent=True))
    time.sleep(1)
