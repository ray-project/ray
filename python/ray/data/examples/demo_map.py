import time
import ray

ray.init()

ds = ray.data.from_items(range(200))


def slow(x):
    time.sleep(0.1)
    return x


ds.map(slow)
