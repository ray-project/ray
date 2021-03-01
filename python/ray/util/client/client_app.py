import ray
import time
from typing import Tuple

ray.util.connect("localhost:50051", connection_retries=0)
#ray.init()


@ray.remote
def f():
    time.sleep(99999)
    return "ok"


@ray.remote
def g():
    time.sleep(3)
    return "bye"
