import ray

@ray.remote
def foo(x):
    if x == 1:
        return 1
    return x + ray.get(foo.remote(x-1))

ray.init()

ray.get(foo.remote(10))
