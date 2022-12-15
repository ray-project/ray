import ray

ray.init()


@ray.remote
def f():
    import time

    time.sleep(300)


refs = [f.remote() for _ in range(30000)]
ray.get(refs)
