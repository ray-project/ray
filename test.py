import ray


@ray.remote(num_gpus=1)
def f():
    pass


ray.get(f.remote())
