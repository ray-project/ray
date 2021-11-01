import ray
import numpy as np

ray.init(address='auto')

@ray.remote
def normal_task(large):
    import time
    time.sleep(60 * 60)
    return "normaltask"

large = ray.put(np.zeros(100 * 2**10, dtype=np.int8))
print("=========large id:", large)
obj = normal_task.remote(large)
ray.get(obj)

