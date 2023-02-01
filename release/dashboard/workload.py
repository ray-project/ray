import time

import ray

ray.init("auto")


@ray.remote(num_cpus=1)
class A:
    def f(self):
        return 1


actors = [A.remote() for _ in range(85)]

# Keep calling actor methods which will generate lots of metrics.
while True:
    time.sleep(0.1)
    ray.get([actor.f.remote() for actor in actors])
