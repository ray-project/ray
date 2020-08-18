import ray

from time import sleep

@ray.remote(resources={"Custom1": 1})
def foo():
    while True:
        print("boop")
        sleep(1)

ray.init(address="auto")

ray.get(foo.remote())
