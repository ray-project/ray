import time

import ray


@ray.remote
class A:
    def f(self, input_obj):
        pass


a = A.remote()
in_ref = ray.put(b"00000000")
ray.get(a.f.remote(in_ref))

start = time.time()
for _ in range(1000):
    ray.get(a.f.remote(in_ref))
print(time.time() - start)
