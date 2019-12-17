# This workload tests submitting and getting many tasks over and over.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

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

cluster = Cluster()
for i in range(num_nodes):
    cluster.add_node(
        redis_port=6379 if i == 0 else None,
        num_redis_shards=num_redis_shards if i == 0 else None,
        num_cpus=2,
        num_gpus=0,
        resources={str(i): 2},
        object_store_memory=object_store_memory,
        redis_max_memory=redis_max_memory)
ray.init(address=cluster.address)

# Run the workload.


@ray.remote
def f(*xs):
    return np.zeros(1024, dtype=np.uint8)


iteration = 0
ids = []
start_time = time.time()
previous_time = start_time
while True:
    for _ in range(50):
        new_constrained_ids = [
            f._remote(args=[*ids], resources={str(i % num_nodes): 1})
            for i in range(25)
        ]
        new_unconstrained_ids = [f.remote(*ids) for _ in range(25)]
        ids = new_constrained_ids + new_unconstrained_ids

    ray.get(ids)

    new_time = time.time()
    print("Iteration {}:\n"
          "  - Iteration time: {}.\n"
          "  - Absolute time: {}.\n"
          "  - Total elapsed time: {}.".format(
              iteration, new_time - previous_time, new_time,
              new_time - start_time))
    previous_time = new_time
    iteration += 1
