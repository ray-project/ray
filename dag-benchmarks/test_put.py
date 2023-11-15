import ray
import numpy as np
import time

ray.init()
arr = np.random.rand(1)
x = ray.put(arr)
assert np.array_equal(ray.get(x), arr)
print("starting...")

for _ in range(10):
    start = time.time()
    for _ in range(10_000):
        x = ray.put(arr)
        #assert (ray.get(x) == arr).all()
    end = time.time()
    print(f"done, tput: {10_000 / (end - start)} puts/s")
