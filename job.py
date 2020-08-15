import ray
from time import sleep

ray.init(address="auto")


@ray.remote(resources={"custom1": 1})
def foo():
    while True:
        print("boop")
        sleep(1)
        pass


@ray.remote(num_gpus=1, resources={"custom2": 2})
def bar():
    while True:
        print("boop")
        sleep(1)
        pass


print("req1")
ref1 = foo.remote()
print("req2")
ref2 = bar.remote()
print("getting")
ray.get([ref1, ref2])
