# This workload tests repeatedly killing a node and adding a new node.

import time

import ray
from ray.cluster_utils import Cluster

num_redis_shards = 5
redis_max_memory = 10**8
object_store_memory = 10**8
num_nodes = 10

message = ("Make sure there is enough memory on this machine to run this "
           "workload. We divide the system memory by 2 to provide a buffer.")
assert (num_nodes * object_store_memory + num_redis_shards * redis_max_memory <
        ray.utils.get_system_memory() / 2), message

# Simulate a cluster on one machine.

cluster = Cluster()
for i in range(num_nodes):
    cluster.add_node(
        redis_port=6379 if i == 0 else None,
        num_redis_shards=num_redis_shards if i == 0 else None,
        num_cpus=2,
        num_gpus=0,
        resources={str(i): 2},
        object_store_memory=object_store_memory,
        redis_max_memory=redis_max_memory,
        webui_host="0.0.0.0")
ray.init(address=cluster.address)

# Run the workload.


@ray.remote
def f(*xs):
    return 1


iteration = 0
previous_ids = [1 for _ in range(100)]
start_time = time.time()
previous_time = start_time
while True:
    for _ in range(100):
        previous_ids = [f.remote(previous_id) for previous_id in previous_ids]

    ray.get(previous_ids)

    for _ in range(100):
        previous_ids = [f.remote(previous_id) for previous_id in previous_ids]

    node_to_kill = cluster.list_all_nodes()[1]
    # Remove the first non-head node.
    cluster.remove_node(node_to_kill)
    cluster.add_node()

    new_time = time.time()
    print("Iteration {}:\n"
          "  - Iteration time: {}.\n"
          "  - Absolute time: {}.\n"
          "  - Total elapsed time: {}.".format(
              iteration, new_time - previous_time, new_time,
              new_time - start_time))
    previous_time = new_time
    iteration += 1
