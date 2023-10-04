import time
import ray

ray.init()

@ray.remote
class A:
    async def f(self):
        for i in range(10):
            print(i)
            time.sleep(1)
            yield 1

a = A.remote()
ref = a.f.options(num_returns="streaming").remote()
time.sleep(3.3)
ray.cancel(ref)
time.sleep(100)
