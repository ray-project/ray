import time
import ray

def f(x):
    time.sleep(1000)
    return x

ray.data.range(1000).map_batches(f).materialize()

