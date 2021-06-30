import time
import ray

ray.init()

ds = ray.experimental.data.from_items(range(200))


def slow(x):
    time.sleep(.1)
    return x


ds.map(slow)
