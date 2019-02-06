#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np
import sys
import time
import json

import ray

logger = logging.getLogger(__name__)

LOCAL = len(sys.argv) > 1

# Start the Ray processes.
if LOCAL:
    print("Starting test locally...")
    from ray.test.cluster_utils import Cluster
    cluster = Cluster(
        initialize_head=True,
        head_node_args={
            "num_cpus": 10,
            "_internal_config": json.dumps({
                "initial_reconstruction_timeout_milliseconds": 1000,
                "num_heartbeats_timeout": 10,
            })
        })
    num_nodes = 10
    for i in range(num_nodes - 1):
        node_to_kill = cluster.add_node(
            num_cpus=10,
            _internal_config=json.dumps({
                "initial_reconstruction_timeout_milliseconds": 1000,
                "num_heartbeats_timeout": 10,
            }))
    ray.init(redis_address=cluster.redis_address)
else:
    num_nodes = 100
    ray.init(redis_address="localhost:6379")


# Require 1 GPU to force the tasks to be on remote machines.
@ray.remote(num_gpus=1)
def shuffle(array):
    return array + 1


@ray.remote
def zeros():
    if LOCAL:
        return np.zeros(10**1)
    else:
        # Create some data of size ~8MB.
        return np.zeros(10**6)


# Run shuffles for 60s. This shuffles about 2 * 100 * 8MB = 1.6GB/round.
data = [zeros.remote() for _ in range(num_nodes * 2)]
num_rounds = 0
start_time = time.time()
round_start = start_time
num_nodes_to_kill = num_nodes // 10
while time.time() - start_time < 300:
    data = [shuffle.remote(array) for array in data]
    num_rounds += 1

    if num_rounds % 10 == 0:
        print("Waiting for round", num_rounds)
        ray.wait(data, num_returns=len(data))
        print("Finished round", num_rounds, "in", time.time() - round_start)
        round_start = time.time()

        # Kill some nodes.
        if LOCAL and num_rounds > 100 and num_nodes_to_kill > 0:
            node_to_kill = cluster.list_all_nodes()[-1]
            cluster.remove_node(node_to_kill)
            num_nodes_to_kill -= 1

# Check that the output is correct.
for array in data:
    assert all(ray.get(array) == num_rounds)
