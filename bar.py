import time

import ray


@ray.remote
def inc():
    time.sleep(0.2)
    return 1


@ray.remote
def add(x):
    time.sleep(0.2)
    return x + 1


@ray.remote
def dec(x):
    time.sleep(0.2)
    return x - 1


@ray.remote
def sum(*x):
    s = 0
    for v in x:
        s += v
    return s


result = sum.remote(*[dec.remote(add.remote(inc.remote())) for _ in range(100)])

ray.progress_bar()
print(ray.get(result))
