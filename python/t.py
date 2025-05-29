import threading
from threading import Thread

import ray

ray.init()

@ray.remote
def f():
    pass

def _submit():
    while True:
        pg = ray.util.placement_group([{"CPU": 0.001}])

threads = [Thread(target=_submit) for _ in range(10)]
[t.start() for t in threads]

import time;time.sleep(5)
print("Shutting down")
ray.shutdown()
print("Shut down!")
