import ray

@ray.remote
def f():
    import time
    time.sleep(1)
    print("F")
    ray.get(f.remote())

ray.get(f.remote())
