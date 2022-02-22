import ray
from ray import serve

import time

ray.init(address="auto")
serve.start()


@serve.deployment
def f():
    time.sleep(1)


f.deploy()

handle = f.get_handle()
while True:
    ray.get(handle.remote())
