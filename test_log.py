import ray
import random

@ray.remote
def f():
    while True:
        print(random.random())

ray.get(f.remote())
