import ray
import time

ray.init()

ds = ray.experimental.data.from_items(range(10000))

setup = False


def infer(x):
    global setup
    if not setup:
        print("setup model")
        setup = True
    if x % 100 == 0:
        print("infer", x)
    time.sleep(.01)
    return x


print(ds.count())
print(ds.sum())
ds = ds.map(infer, compute="actors", num_cpus=0.5)
ds.show()
