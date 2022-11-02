# This workload tests submitting and getting many tasks over and over.
import json
import os
import time

import numpy as np

import ray
from ray.cluster_utils import Cluster


def update_progress(result):
    result["last_update"] = time.time()
    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/release_test_output.json"
    )
    with open(test_output_json, "wt") as f:
        json.dump(result, f)


num_redis_shards = 5
redis_max_memory = 10**8
object_store_memory = 10**8
num_nodes = 10

message = (
    "Make sure there is enough memory on this machine to run this "
    "workload. We divide the system memory by 2 to provide a buffer."
)
assert (
    num_nodes * object_store_memory + num_redis_shards * redis_max_memory
    < ray._private.utils.get_system_memory() / 2
), message

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
        dashboard_host="0.0.0.0",
    )
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
            f._remote(args=[*ids], resources={str(i % num_nodes): 1}) for i in range(25)
        ]
        new_unconstrained_ids = [f.remote(*ids) for _ in range(25)]
        ids = new_constrained_ids + new_unconstrained_ids

    ray.get(ids)

    new_time = time.time()
    print(
        "Iteration {}:\n"
        "  - Iteration time: {}.\n"
        "  - Absolute time: {}.\n"
        "  - Total elapsed time: {}.".format(
            iteration, new_time - previous_time, new_time, new_time - start_time
        )
    )
    update_progress(
        {
            "iteration": iteration,
            "iteration_time": new_time - previous_time,
            "absolute_time": new_time,
            "elapsed_time": new_time - start_time,
        }
    )
    previous_time = new_time
    iteration += 1
