import ray
from ray import serve
from ray.util import metrics

import time

ray.init(address="auto")
serve.start()


@serve.deployment
class MyDeployment:
    def __init__(self):
        self.my_counter = metrics.Counter(
            "my_counter",
            description=("The number of excellent requests to this backend."),
            tag_keys=("deployment", ))
        self.my_counter.set_default_tags({
            "deployment": serve.get_current_deployment()
        })

    def call(self, excellent=False):
        if excellent:
            self.my_counter.inc()


MyDeployment.deploy()

handle = MyDeployment.get_handle()
while True:
    ray.get(handle.call.remote(excellent=True))
    time.sleep(1)
