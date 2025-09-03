import time

import ray

@ray.remote
def f(o):
    ray.get(o[0])

while True:
    f.remote([ray.put("hi")])
    time.sleep(1)
