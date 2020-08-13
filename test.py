import ray
from time import sleep


@ray.remote
def spin():
    while True:
        pass

ray.init(address="auto")

refs = [spin.remote() for _ in range(20)]

while True:
    print(ray.cluster_resources())
    sleep(5)
