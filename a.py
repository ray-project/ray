import ray
ray.init()

@ray.remote
class A:
    def run(self):
        return 1

actors = [A.remote() for _ in range(100)]
while True:
    import time
    time.sleep(0.5)
    ray.get([a.run.remote() for a in actors])
