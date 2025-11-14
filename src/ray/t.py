import ray

@ray.remote(num_returns="streaming")
def f():
    yield "hi"

o = f.remote()
ray.get(next(o))
ray.get(next(o))
