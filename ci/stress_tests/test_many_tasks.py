#!/usr/bin/env python

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


# Stage 0: Submit a bunch of small tasks with large returns.
stage_0_iterations = []
start_time = time.time()
logger.info("Submitting many tasks with large returns.")
for i in range(10):
    iteration_start = time.time()
    logger.info("Iteration %s", i)
    ray.get([f.remote(1000000) for _ in range(1000)])
    stage_0_iterations.append(time.time() - iteration_start)

stage_0_time = time.time() - start_time
logger.info("Finished stage 0 after %s seconds.", stage_0_time)

# Stage 1: Launch a bunch of tasks.
stage_1_iterations = []
start_time = time.time()
logger.info("Submitting many tasks.")
for i in range(10):
    iteration_start = time.time()
    logger.info("Iteration %s", i)
    ray.get([f.remote(0) for _ in range(100000)])
    stage_1_iterations.append(time.time() - iteration_start)

stage_1_time = time.time() - start_time
logger.info("Finished stage 1 after %s seconds.", stage_1_time)

# Launch a bunch of tasks, each with a bunch of dependencies. TODO(rkn): This
# test starts to fail if we increase the number of tasks in the inner loop from
# 500 to 1000. (approximately 615 seconds)
stage_2_iterations = []
start_time = time.time()
logger.info("Submitting tasks with many dependencies.")
x_ids = []
for _ in range(5):
    iteration_start = time.time()
    for i in range(20):
        logger.info("Iteration %s. Cumulative time %s seconds", i,
                    time.time() - start_time)
        x_ids = [f.remote(0, *x_ids) for _ in range(500)]
    ray.get(x_ids)
    stage_2_iterations.append(time.time() - iteration_start)
    logger.info("Finished after %s seconds.", time.time() - start_time)

stage_2_time = time.time() - start_time
logger.info("Finished stage 2 after %s seconds.", stage_2_time)

# Create a bunch of actors.
start_time = time.time()
logger.info("Creating %s actors.", num_remote_cpus)
actors = [Actor.remote() for _ in range(num_remote_cpus)]
stage_3_creation_time = time.time() - start_time
logger.info("Finished stage 3 actor creation in %s seconds.",
            stage_3_creation_time)

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
stage_3_time = time.time() - start_time
logger.info("Finished stage 3 in %s seconds.", stage_3_time)

print("Stage 0 results:")
print("\tTotal time: {}".format(stage_0_time))

print("Stage 1 results:")
print("\tTotal time: {}".format(stage_1_time))
print("\tAverage iteration time: {}".format(
    sum(stage_1_iterations) / len(stage_1_iterations)))
print("\tMax iteration time: {}".format(max(stage_1_iterations)))
print("\tMin iteration time: {}".format(min(stage_1_iterations)))

print("Stage 2 results:")
print("\tTotal time: {}".format(stage_2_time))
print("\tAverage iteration time: {}".format(
    sum(stage_2_iterations) / len(stage_2_iterations)))
print("\tMax iteration time: {}".format(max(stage_2_iterations)))
print("\tMin iteration time: {}".format(min(stage_2_iterations)))

print("Stage 3 results:")
print("\tActor creation time: {}".format(stage_3_creation_time))
print("\tTotal time: {}".format(stage_3_time))

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
