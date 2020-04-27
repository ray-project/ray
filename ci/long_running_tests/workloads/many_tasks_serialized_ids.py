# This workload stresses distributed reference counting by passing and
# returning serialized ObjectIDs.

import time
import json
import random

import numpy as np

import ray
from ray.cluster_utils import Cluster

num_redis_shards = 5
redis_max_memory = 10**8
object_store_memory = 10**8
num_nodes = 10

message = ("Make sure there is enough memory on this machine to run this "
           "workload. We divide the system memory by 2 to provide a buffer.")
assert (num_nodes * object_store_memory + num_redis_shards * redis_max_memory <
        ray.utils.get_system_memory() / 2)

# Simulate a cluster on one machine.

config = json.dumps({
    "distributed_ref_counting_enabled": 1,
})
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
        webui_host="0.0.0.0",
        _internal_config=config,
    )
ray.init(address=cluster.address)

# Run the workload.


@ray.remote(max_retries=0)
def churn():
    return ray.put(np.zeros(1024 * 1024, dtype=np.uint8))


@ray.remote(max_retries=0)
def child(*xs):
    oid = ray.put(np.zeros(1024 * 1024, dtype=np.uint8))
    return oid


@ray.remote(max_retries=0)
def f(*xs):
    if xs:
        return random.choice(xs)
    else:
        return child.remote(*xs)


iteration = 0
ids = []
start_time = time.time()
previous_time = start_time
while True:
    for _ in range(50):
        new_constrained_ids = [
            f._remote(args=ids, resources={str(i % num_nodes): 1})
            for i in range(25)
        ]
        new_unconstrained_ids = [f.remote(*ids) for _ in range(25)]
        ids = new_constrained_ids + new_unconstrained_ids

    # Fill the object store while the tasks are running.
    for i in range(num_nodes):
        for _ in range(10):
            [
                churn._remote(args=[], resources={str(i % num_nodes): 1})
                for _ in range(10)
            ]

    # Make sure that the objects are still available.
    child_ids = ray.get(ids)
    for child_id in child_ids:
        ray.get(child_id)

    new_time = time.time()
    print("Iteration {}:\n"
          "  - Iteration time: {}.\n"
          "  - Absolute time: {}.\n"
          "  - Total elapsed time: {}.".format(
              iteration, new_time - previous_time, new_time,
              new_time - start_time))
    previous_time = new_time
    iteration += 1
