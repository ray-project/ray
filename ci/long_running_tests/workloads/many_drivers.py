# This workload tests many drivers using the same cluster.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import ray
from ray.tests.cluster_utils import Cluster
from ray.tests.utils import run_string_as_driver

num_redis_shards = 5
redis_max_memory = 10**8
object_store_memory = 10**8
num_nodes = 4

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
        num_cpus=4,
        num_gpus=0,
        resources={str(i): 5},
        object_store_memory=object_store_memory,
        redis_max_memory=redis_max_memory)
ray.init(redis_address=cluster.redis_address)

# Run the workload.

# Define a driver script that runs a few tasks and actors on each node in the
# cluster.
driver_script = """
import ray

ray.init(redis_address="{}")

num_nodes = {}


@ray.remote
def f():
    return 1


@ray.remote
class Actor(object):
    def method(self):
        return 1


for _ in range(5):
    for i in range(num_nodes):
        assert (ray.get(
            f._remote(args=[], kwargs={{}}, resources={{str(i): 1}})) == 1)
        actor = Actor._remote(args=[], kwargs={{}}, resources={{str(i): 1}})
        assert ray.get(actor.method.remote()) == 1

print("success")
""".format(cluster.redis_address, num_nodes)


@ray.remote
def run_driver():
    output = run_string_as_driver(driver_script)
    assert "success" in output


iteration = 0
running_ids = [
    run_driver._remote(
        args=[], kwargs={}, num_cpus=0, resources={str(i): 0.01})
    for i in range(num_nodes)
]
start_time = time.time()
previous_time = start_time
while True:
    # Wait for a driver to finish and start a new driver.
    [ready_id], running_ids = ray.wait(running_ids, num_returns=1)
    ray.get(ready_id)

    running_ids.append(
        run_driver._remote(
            args=[],
            kwargs={},
            num_cpus=0,
            resources={str(iteration % num_nodes): 0.01}))

    new_time = time.time()
    print("Iteration {}:\n"
          "  - Iteration time: {}.\n"
          "  - Absolute time: {}.\n"
          "  - Total elapsed time: {}.".format(
              iteration, new_time - previous_time, new_time,
              new_time - start_time))
    previous_time = new_time
    iteration += 1
