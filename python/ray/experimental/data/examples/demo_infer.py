import ray
import time

ray.init()

ds = ray.experimental.data.from_items(range(10000))

setup = False


def infer(x):
    global setup
    if not setup:
        setup = True
    time.sleep(.01)
    return x


print(ds.count())
ds = ds.map(infer, compute="actors", num_cpus=0.5)
ds.show()
