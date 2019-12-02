import ray

from ray.cluster_utils import Cluster

cluster = Cluster(
    initialize_head=True, connect=True, head_node_args={"num_cpus": 0})
cluster.add_node(num_cpus=1)

# Test a simple function.


@ray.remote
def f_simple():
    return 1


for i in range(10):
    print(i)
    assert ray.get(f_simple.remote()) == 1
