import ray
from ray.cluster_utils import Cluster
import numpy as np

num_redis_shards = 1
redis_max_memory = 10**8
object_store_memory = 10**8
num_nodes = 1

# Simulate a cluster on one machine.
cluster = Cluster()
for i in range(num_nodes):
    cluster.add_node(
        redis_port=6379 if i == 0 else None,
        num_redis_shards=num_redis_shards if i == 0 else None,
        num_cpus=4,
        num_gpus=0,
        resources={str(i): 2},
        object_store_memory=object_store_memory,
        redis_max_memory=redis_max_memory,
        webui_host="0.0.0.0")
ray.init(address=cluster.address)
print(ray.nodes())


@ray.remote
class Actor:
    def __init__(self):
        self.obj = []

    def add_object(self):
        obj = 1
        obj = ray.put(np.zeros(100))
        self.obj.append(obj)
        return obj


a = Actor.remote()
a2 = Actor.remote()
a3 = Actor.remote()
a4 = Actor.remote()

print(a)
result = a.add_object.remote()
a = ray.put(np.zeros(1))
b = ray.get(a)
del a

while True:
    import time
    time.sleep(10.0)
