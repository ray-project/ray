import ray
import time

while True:
    s = time.time()
    ray.init()
    @ray.remote
    class A:
        def f(self):
            for _ in range(300):
                12 + 12

    actors = [A.remote() for _ in range(8)]
    for _ in range(30):
        [ray.get(c.f.remote()) for c in actors]
    for actor in actors:
        ray.kill(actor)
    for actor in actors:
        try:
            ray.get(c.__ray_ready__.remote())
        except Exception:
            pass
    ray.shutdown()
    print(time.time() - s)
