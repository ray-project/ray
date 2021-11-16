import ray
from ray.cluster_utils import Cluster
from ray.util.placement_group import placement_group

c = Cluster()

c.add_node(num_cpus=4)
ray.init("auto")
for _ in range(3):
    c.add_node(num_cpus=4)

@ray.remote(num_cpus=4)
class Worker:
    def process(self):
        pass

@ray.remote(num_cpus=1)
def train():
    pg = ray.util.placement_group(bundles=[{"CPU":4} for _ in range(4)], strategy="SPREAD")
    ray.get(pg.ready())
    workers = [Worker.options(placement_group=pg).remote() for _ in range(4)]
    ray.get([worker.process.remote() for worker in workers])
    # import time
    # print("Done")
    # time.sleep(180)

ray.get(train.remote())
ray.shutdown()
c.shutdown()