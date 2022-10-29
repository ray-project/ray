import ray
ray.init()

@ray.remote
def f():
    import time
    time.sleep(30)

a = f.remote()
b = f.options(num_gpus=1).remote()
ray.get(a)
