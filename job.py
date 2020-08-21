import ray
import os
from time import sleep

ray.init(address="auto")

@ray.remote(num_gpus=1)
def foo():
    while True:
        print(os.listdir("~"))
        sleep(1)

ray.get(foo.remote())
