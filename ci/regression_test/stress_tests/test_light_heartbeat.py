#!/usr/bin/env python

import logging
import os
import time

import ray
import psutil
from statistics import mean

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


# Require 1 GPU to force the actors to be on remote machines.
@ray.remote(num_cpus=1, num_gpus=1)
class Actor(object):
    def network_stats(self):
        ifaces = [
            v for k, v in psutil.net_io_counters(pernic=True).items()
            if k[0] == "e"
        ]

        recv = sum((iface.bytes_recv for iface in ifaces))
        return recv

    def cpu_load(self):
        # Get cpu load average in last 1 minite.
        return os.getloadavg()[0]


# Create a bunch of actors.
logger.info("Creating %s actors.", num_remote_cpus)
actors = [Actor.remote() for _ in range(num_remote_cpus)]
before_newtwork_counts = mean(
    ray.get([a.network_stats.remote() for a in actors]))

# Sleep for 1 minite to gather statistics.
time.sleep(60)

stats = ray.get([a.cpu_load.remote() for a in actors])
logger.info("Average cpu load of all nodes is %s.", mean(stats))

after_newtwork_counts = mean(
    ray.get([a.network_stats.remote() for a in actors]))
delta_network_counts = after_newtwork_counts - before_newtwork_counts
logger.info("Averaged bytes received of all nodes is %s.",
            delta_network_counts)

print("Average cpu load: {}".format(mean(stats)))
print("Averaged bytes received: {}".format(delta_network_counts))
