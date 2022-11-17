import ray
ray.init()
@ray.remote
class A:
    def run(self):
        import numpy as np
        import time
        for _ in range(10000):
            time.sleep(0.1)
            arr = np.random.rand(5 * 1024 * 1024)

a = A.remote()
ray.get(a.run.remote())
import time
time.sleep(30)
