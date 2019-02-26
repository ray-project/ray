# This workload tests submitting and getting many tasks over and over.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import ray
from ray.tests.cluster_utils import Cluster

num_redis_shards = 5
redis_max_memory = 10**8
object_store_memory = 10**8
num_nodes = 10

# Make sure there is enough memory on this machine to run this workload. We
# divide the system memory by 2 to provide a buffer.
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
ray.init(redis_address=cluster.redis_address)

# Run the workload.


@ray.remote
def f(*xs):
    return 1


iteration = 0
ids = []
previous_time = time.time()
while True:
    new_time = time.time()
    print("Iteration {}. Elapsed time is {}.".format(iteration,
                                                     new_time - previous_time))
    previous_time = new_time
    new_constrained_ids = [
        f._remote(args=[*ids], resources={str(i % num_nodes): 1})
        for i in range(10)
    ]
    new_unconstrained_ids = [f.remote(*ids) for _ in range(10)]
    ids = new_constrained_ids + new_unconstrained_ids

    if iteration % 50 == 0:
        ray.get(ids)

    iteration += 1
