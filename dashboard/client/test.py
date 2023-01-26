import ray
ray.init()

@ray.remote
def f():
    pass

ray.get([f.remote() for _ in range(10)])