import ray

ray.init()

@ray.remote
class A:
    def f(self):
        pass
a = A.remote()
