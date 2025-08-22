import ray
from ray.cluster_utils import Cluster
import time
import numpy as np

cluster = Cluster()

head = cluster.add_node(num_cpus=0)
ray.init(address=cluster.address)
worker = cluster.add_node(num_cpus=1)


@ray.remote
class A:
    def __init__(self):
        self.arr = np.zeros(4 * 1024 * 1024, dtype=np.uint8)

    def generator(self):
        for _ in range(100):
            yield self.arr


a = A.remote()
ray.get(a.__ray_ready__.remote())

start_time = time.time()

streaming_ref = a.generator.remote()
for ref in streaming_ref:
    ray.get(ref)


print("really done in ", time.time() - start_time)
