import ray
import numpy as np


ray.init(num_cpus=4)

def sleep(x):
    import time
    time.sleep(1)
    return x

for x in ray.data.range(50, parallelism=20) \
        .map(sleep, num_cpus=0.3) \
        .map(sleep, num_cpus=0.4) \
        .map(sleep, num_cpus=0.5) \
        .iter_rows():
    pass
#    print("OUTPUT", x)
