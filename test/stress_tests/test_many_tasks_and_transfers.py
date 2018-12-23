#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import logging
import time

import ray

logger = logging.getLogger(__name__)

ray.init(redis_address="localhost:6379")

# These numbers need to match the values in the autoscaler config file.
num_remote_nodes = 100
head_node_cpus = 2
num_remote_cpus = num_remote_nodes * head_node_cpus

# Wait until the expected number of nodes have joined the cluster.
while True:
    if len(ray.global_state.client_table()) >= num_remote_nodes + 1:
        break
logger.info("Nodes have all joined. There are {} resources."
            .format(ray.global_state.cluster_resources()))


# Require 1 GPU to force the tasks to be on remote machines.
@ray.remote(num_gpus=1)
def f(size, *xs):
    return np.ones(size, dtype=np.uint8)


# Require 1 GPU to force the actors to be on remote machines.
@ray.remote(num_cpus=1, num_gpus=1)
class Actor(object):
    def method(self, size, *xs):
        return np.ones(size, dtype=np.uint8)


# Launch a bunch of tasks. (approximately 200 seconds)
start_time = time.time()
logger.info("Submitting many tasks.")
for i in range(10):
    logger.info("Iteration {}".format(i))
    ray.get([f.remote(0) for _ in range(100000)])
logger.info("Finished after {} seconds.".format(time.time() - start_time))

# Launch a bunch of tasks, each with a bunch of dependencies. TODO(rkn): This
# test starts to fail if we increase the number of tasks in the inner loop from
# 500 to 1000. (approximately 660 seconds)
start_time = time.time()
logger.info("Submitting tasks with many dependencies.")
x_ids = []
for i in range(100):
    logger.info("Iteration {}. Cumulative time {} seconds".format(
        i,
        time.time() - start_time))
    logger.info("Iteration {}".format(i))
    x_ids = [f.remote(0, *x_ids) for _ in range(500)]
ray.get(x_ids)
logger.info("Finished after {} seconds.".format(time.time() - start_time))

# Create a bunch of actors.
start_time = time.time()
logger.info("Creating {} actors.".format(num_remote_cpus))
actors = [Actor.remote() for _ in range(num_remote_cpus)]
logger.info("Finished after {} seconds.".format(time.time() - start_time))

# Submit a bunch of small tasks to each actor. (approximately 1070 seconds)
start_time = time.time()
logger.info("Submitting many small actor tasks.")
x_ids = []
for _ in range(100000):
    x_ids = [a.method.remote(0) for a in actors]
ray.get(x_ids)
logger.info("Finished after {} seconds.".format(time.time() - start_time))

# # Submit a bunch of actor tasks with all-to-all communication.
# start_time = time.time()
# logger.info("Submitting actor tasks with all-to-all communication.")
# x_ids = []
# for _ in range(50):
#     for size_exponent in [0, 1, 2, 3, 4, 5, 6]:
#         x_ids = [a.method.remote(10**size_exponent, *x_ids) for a in actors]
# ray.get(x_ids)
# logger.info("Finished after {} seconds.".format(time.time() - start_time))
