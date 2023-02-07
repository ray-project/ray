import ray

ray.init()

@ray.remote
def f():
    import time
    time.sleep(5)

ray.get(f.remote())
