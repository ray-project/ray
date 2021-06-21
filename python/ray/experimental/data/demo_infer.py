import ray
import time
import os

ray.init()

ds = ray.experimental.data.range(10000)

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

ds = ds.map(infer, compute="actors", num_cpus=0.5)

print(ds.count())
print(ds.sum())
ds.show()
