import os
import subprocess

import ray
from ray.cluster_utils import Cluster

import psutil

cluster = Cluster()

node1 = cluster.add_node(True, num_cpus=4, num_gpus=0)

ray.init(cluster.address)

node2 = cluster.add_node(True, num_gpus=4, num_cpus=0)


n = {n["NodeManagerPort"] for n in ray.nodes()}
n.remove(node1.node_manager_port)

n = list(n)[0]


@ray.remote(num_cpus=0, num_gpus=1, num_returns=2)
def f():
    return ray.put(os.getpid()), ray.put(b"x" * 1024 * 1024)


@ray.remote(num_cpus=0, num_gpus=1)
class A:
    def __init__(self):
        self.data = ray.put(b"x" * 1024 * 1024)

    def fetch(self):
        return self.data


a = A.remote()

pid, obj = f.remote()

p = pid

pid = ray.get(ray.get(pid))

a_data = a.fetch.remote()

subprocess.run(
    f"grpcurl -use-reflection -plaintext localhost:{n} ray.rpc.NodeManagerService.DrainObjectStore",
    shell=True,
)


# proc = psutil.Process(pid)
# proc.kill()

cluster.remove_node(node2)


@ray.remote(num_gpus=0, num_cpus=1)
def g(o):
    print("!!!", os.getpid())
    print("GET:", o)
    ray.get(o)
    print("SUCC")


print(ray.get(g.remote(obj)))
print(ray.get(ray.get(a_data))[0:10])
