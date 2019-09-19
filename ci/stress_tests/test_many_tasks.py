#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import logging
import time

import ray

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ray.init(address="localhost:6379")

# These numbers need to correspond with the autoscaler config file.
# The number of remote nodes in the autoscaler should upper bound
# these because sometimes nodes fail to update.
num_remote_nodes = 100
head_node_cpus = 2
num_remote_cpus = num_remote_nodes * head_node_cpus

# Wait until the expected number of nodes have joined the cluster.
while True:
    num_nodes = len(ray.nodes())
    logger.info("Waiting for nodes {}/{}".format(num_nodes,
                                                 num_remote_nodes + 1))
    if num_nodes >= num_remote_nodes + 1:
        break
    time.sleep(5)
logger.info("Nodes have all joined. There are %s resources.",
            ray.cluster_resources())


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
    logger.info("Iteration %s", i)
    ray.get([f.remote(0) for _ in range(100000)])
logger.info("Finished after %s seconds.", time.time() - start_time)

# Launch a bunch of tasks, each with a bunch of dependencies. TODO(rkn): This
# test starts to fail if we increase the number of tasks in the inner loop from
# 500 to 1000. (approximately 615 seconds)
start_time = time.time()
logger.info("Submitting tasks with many dependencies.")
x_ids = []
for _ in range(5):
    for i in range(20):
        logger.info("Iteration %s. Cumulative time %s seconds", i,
                    time.time() - start_time)
        x_ids = [f.remote(0, *x_ids) for _ in range(500)]
    ray.get(x_ids)
    logger.info("Finished after %s seconds.", time.time() - start_time)

# Create a bunch of actors.
start_time = time.time()
logger.info("Creating %s actors.", num_remote_cpus)
actors = [Actor.remote() for _ in range(num_remote_cpus)]
logger.info("Finished after %s seconds.", time.time() - start_time)

# Submit a bunch of small tasks to each actor. (approximately 1070 seconds)
start_time = time.time()
logger.info("Submitting many small actor tasks.")
for N in [1000, 100000]:
    x_ids = []
    for i in range(N):
        x_ids = [a.method.remote(0) for a in actors]
        if i % 100 == 0:
            logger.info("Submitted {}".format(i * len(actors)))
    ray.get(x_ids)
logger.info("Finished after %s seconds.", time.time() - start_time)

# TODO(rkn): The test below is commented out because it currently does not
# pass.
# # Submit a bunch of actor tasks with all-to-all communication.
# start_time = time.time()
# logger.info("Submitting actor tasks with all-to-all communication.")
# x_ids = []
# for _ in range(50):
#     for size_exponent in [0, 1, 2, 3, 4, 5, 6]:
#         x_ids = [a.method.remote(10**size_exponent, *x_ids) for a in actors]
# ray.get(x_ids)
# logger.info("Finished after %s seconds.", time.time() - start_time)
