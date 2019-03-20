# This workload tests repeatedly killing actors and submitting tasks to them.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import sys
import time

import ray
from ray.tests.cluster_utils import Cluster

num_redis_shards = 1
redis_max_memory = 10**8
object_store_memory = 10**8
num_nodes = 2

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
        num_cpus=8,
        num_gpus=0,
        resources={str(i): 2},
        object_store_memory=object_store_memory,
        redis_max_memory=redis_max_memory)
ray.init(redis_address=cluster.redis_address)

# Run the workload.

num_parents = 5
num_children = 5
death_probability = 0.95


@ray.remote
class Child(object):
    def __init__(self, death_probability):
        self.death_probability = death_probability

    def ping(self):
        # Exit process with some probability.
        exit_chance = np.random.rand()
        if exit_chance > self.death_probability:
            sys.exit(-1)


@ray.remote
class Parent(object):
    def __init__(self, num_children, death_probability):
        self.death_probability = death_probability
        self.children = [
            Child.remote(death_probability) for _ in range(num_children)
        ]

    def ping(self, num_pings):
        children_outputs = []
        for _ in range(num_pings):
            children_outputs += [
                child.ping.remote() for child in self.children
            ]
        try:
            ray.get(children_outputs)
        except Exception:
            # Replace the children if one of them died.
            self.__init__(len(self.children), self.death_probability)

    def kill(self):
        # Clean up children.
        ray.get([child.__ray_terminate__.remote() for child in self.children])


parents = [
    Parent.remote(num_children, death_probability) for _ in range(num_parents)
]

iteration = 0
start_time = time.time()
previous_time = start_time
while True:
    ray.get([parent.ping.remote(10) for parent in parents])

    # Kill a parent actor with some probability.
    exit_chance = np.random.rand()
    if exit_chance > death_probability:
        parent_index = np.random.randint(len(parents))
        parents[parent_index].kill.remote()
        parents[parent_index] = Parent.remote(num_children, death_probability)

    new_time = time.time()
    print("Iteration {}:\n"
          "  - Iteration time: {}.\n"
          "  - Absolute time: {}.\n"
          "  - Total elapsed time: {}.".format(
              iteration, new_time - previous_time, new_time,
              new_time - start_time))
    previous_time = new_time
    iteration += 1
