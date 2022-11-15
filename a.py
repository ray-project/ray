import ray
ray.init()
@ray.remote(num_cpus=1)
class A:
    pass

a = [A.remote() for _ in range(10000)]
import time
time.sleep(300)
