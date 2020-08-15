import ray
from time import sleep

ray.init(address="auto")


@ray.remote(num_gpus=1)
def foo():
    while True:
        print("boop")
        sleep(1)
        pass


ray.get(foo.remote())
