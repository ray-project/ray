import time

import ray


@ray.remote
def model():
    ray.get([subproc.remote() for _ in range(20)])


@ray.remote
def subproc():
    time.sleep(1)


@ray.remote
def main():
    result = [model.remote() for _ in range(5)]
    ray.get(result)


m = main.remote()
ray.progress_bar()
ray.get(m)
