import ray
ray.init()

@ray.remote(max_calls=1)
def f():
    pass

print(ray.get(f.remote()))
