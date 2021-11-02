import ray
ray.init()

@ray.remote
class A:
    def __init__(self):
        self.a = {}
    
    def run(self):
        for i in range(1000000):
            self.a[i] = "X"
            del self.a
    def run_ha(self):
        for j in range(50000, 1000000):
            self.a[j] = "Y"

a = A.options(max_concurrency=10).remote()

ray.get([a.run.remote() for _ in range(100)])


