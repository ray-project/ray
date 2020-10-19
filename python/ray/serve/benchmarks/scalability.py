# A multi-node scalability test. We put an http proxy on the head node and spin
# up 20 nodes and put as many replicas as possible on the cluster, and run a
# stress test.
#
# Test will measure latency and throughput under a high load using `wrk`
# running on each node.
#
# Results for node 1 of 21:
# Running 10s test @ http://127.0.0.1:8000/hey
#   2 threads and 63 connections
#   Thread Stats   Avg      Stdev     Max   +/- Stdev
#     Latency   263.96ms   96.29ms 506.39ms   69.14%
#     Req/Sec   115.63     79.29   650.00     75.40%
#   2307 requests in 10.00s, 315.66KB read
# Requests/sec:    230.61
# Transfer/sec:     31.55KB
#
# Results for node 2 of 21:
# Running 10s test @ http://127.0.0.1:8000/hey
#   2 threads and 63 connections
#   Thread Stats   Avg      Stdev     Max   +/- Stdev
#     Latency   282.79ms   75.00ms 500.26ms   63.32%
#     Req/Sec   108.20     60.17   240.00     58.92%
#   2159 requests in 10.02s, 295.42KB read
# Requests/sec:    215.47
# Transfer/sec:     29.48KB
#
# [...] similar results for remaining nodes

import time
import subprocess
import requests

import ray
from ray import serve
from ray.serve import BackendConfig
from ray.serve.utils import logger

from ray.util.placement_group import (placement_group, remove_placement_group)

ray.shutdown()
ray.init(address="auto")
client = serve.start()

# These numbers need to correspond with the autoscaler config file.
# The number of remote nodes in the autoscaler should upper bound
# these because sometimes nodes fail to update.
num_workers = 20
expected_num_nodes = num_workers + 1
cpus_per_node = 4
num_remote_cpus = expected_num_nodes * cpus_per_node

# Wait until the expected number of nodes have joined the cluster.
while True:
    num_nodes = len(ray.nodes())
    logger.info("Waiting for nodes {}/{}".format(num_nodes,
                                                 expected_num_nodes))
    if num_nodes >= expected_num_nodes:
        break
    time.sleep(5)
logger.info("Nodes have all joined. There are %s resources.",
            ray.cluster_resources())


def hey(_):
    time.sleep(0.01)  # Sleep for 10ms
    return b"hey"


num_connections = int(num_remote_cpus * 0.75)
num_threads = 2
time_to_run = "10s"

pg = placement_group(
    [{
        "CPU": 1
    } for _ in range(expected_num_nodes)], strategy="STRICT_SPREAD")
ray.get(pg.ready())

# The number of replicas is the number of cores remaining after accounting
# for the one HTTP proxy actor on each node, the "hey" requester task on each
# node, and the serve controller.
# num_replicas = expected_num_nodes * (cpus_per_node - 2) - 1
num_replicas = ray.available_resources()["CPU"]
logger.info("Starting %i replicas", num_replicas)
client.create_backend(
    "hey", hey, config=BackendConfig(num_replicas=num_replicas))
client.create_endpoint("hey", backend="hey", route="/hey")


@ray.remote
def run_wrk():
    logger.info("Warming up for ~3 seconds")
    for _ in range(5):
        resp = requests.get("http://127.0.0.1:8000/hey").text
        logger.info("Received response \'" + resp + "\'")
        time.sleep(0.5)

    result = subprocess.run(
        [
            "wrk", "-c",
            str(num_connections), "-t",
            str(num_threads), "-d", time_to_run, "http://127.0.0.1:8000/hey"
        ],
        stdout=subprocess.PIPE)
    return result.stdout.decode()


results = ray.get([
    run_wrk.options(placement_group=pg,
                    placement_group_bundle_index=i).remote()
    for i in range(expected_num_nodes)
])

for i in range(expected_num_nodes):
    logger.info("Results for node %i of %i:", i + 1, expected_num_nodes)
    logger.info(results[i])

remove_placement_group(pg)
