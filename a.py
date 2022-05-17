import ray
ray.init()
import time
@ray.remote(num_cpus=1)
class A:
    pass
a = [A.remote() for _ in range(140)]
time.sleep(300)
