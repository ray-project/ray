#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import logging

import ray

logger = logging.getLogger(__name__)

ray.init(redis_address="localhost:6379")

# Wait until the expected number of nodes have joined the cluster.
while True:
    if len(ray.global_state.client_table()) >= 2:
        break
num_cpus = ray.global_state.cluster_resources["CPU"]
logger.info("Nodes have all joined. There are {} CPU resources."
            .format(num_cpus))


@ray.remote
def f(size, *xs):
    return np.ones(size, dtype=np.uint8)


@ray.remote(num_cpus=1)
class Actor(object):
    def method(self, size, *xs):
        return np.ones(size, dtype=np.uint8)


# Launch a bunch of tasks.
logger.info("Submitting many tasks.")
for _ in range(10):
    ray.get([f.remote(0) for _ in range(100000)])

# Launch a bunch of tasks, each with a bunch of dependencies.
logger.info("Submitting tasks with many dependencies.")
x_ids = []
for _ in range(10):
    x_ids = [f.remote(0, *x_ids) for _ in range(10000)]
ray.get(x_ids)

# Create a bunch of actors.
logger.info("Creating {} actors.".format(num_cpus))
actors = [Actor.remote() for _ in range(num_cpus)]

# Submit a bunch of small tasks to each actor.
logger.info("Submitting many small actor tasks.")
x_ids = []
for _ in range(100000):
    x_ids = [a.method.remote(0) for a in actors]
ray.get(x_ids)

# Submit a bunch of actor tasks with all-to-all communication.
logger.info("Submitting actor tasks with all-to-all communication.")
x_ids = []
for _ in range(100):
    for size_exponent in [0, 1, 2, 3, 4, 5, 6]:
        x_ids = [a.method.remote(10**size_exponent, *x_ids) for a in actors]
ray.get(x_ids)
