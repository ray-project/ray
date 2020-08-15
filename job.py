import ray
from time import sleep

ray.init(address="auto")


@ray.remote(num_gpus=1)
def foo():
    while True:
        print("boop")
        sleep(1)
        pass

print("req1")
ref1 = foo.remote()
print("req2")
ref2 = foo.remote()
print("getting")
ray.get([ref1, ref2])

