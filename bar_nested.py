import time

import ray


@ray.remote
def model():
    ray.get([subproc.remote() for _ in range(20)])


@ray.remote
def subproc():
    time.sleep(0.1)

result = [model.remote() for _ in range(5)]
ray.progress_bar()
ray.get(result)
