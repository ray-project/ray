# A multi-node scalability test. We put an http proxy on the head node and spin
# up 20 nodes and put as many replicas as possible on the cluster, and run a
# stress test.
#
# Test will measure latency and throughput under a high load using `wrk`
# running on each node.
#
# Sample output:
# Running 10s test @ http://127.0.0.1:8000/hey
#   2 threads and 100 connections
#   Thread Stats   Avg      Stdev     Max   +/- Stdev
#     Latency    44.14ms   11.24ms 151.03ms   94.78%
#     Req/Sec     1.15k   185.88     1.36k    91.50%
#   Latency Distribution
#      50%   42.10ms
#      75%   44.78ms
#      90%   49.40ms
#      99%  106.94ms
#   22917 requests in 10.04s, 3.56MB read
# Requests/sec:   2283.17
# Transfer/sec:    363.43KB

import logging
import time
import subprocess
import requests

import ray
from ray import serve
from ray.serve import BackendConfig

from ray.util.placement_group import placement_group

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ray.init(address="auto")
client = serve.start()

# These numbers need to correspond with the autoscaler config file.
# The number of remote nodes in the autoscaler should upper bound
# these because sometimes nodes fail to update.
num_workers = 20
expected_num_nodes = num_workers + 1
cpus_per_node = 2
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


# The number of replicas is the number of cores remaining after accounting
# for the one HTTP proxy actor on each node, the "hey" requester task on each
# node, and the serve controller.
num_replicas = expected_num_nodes * (cpus_per_node - 2) - 1
client.create_backend(
    "hey", hey, config=BackendConfig(num_replicas=num_replicas))
client.create_endpoint("hey", backend="hey", route="/hey")

logger.info("Warming up for ~3 seconds")
for _ in range(5):
    resp = requests.get("http://127.0.0.1:8000/hey").text
    logger.info("Received response \'" + resp + "\'")
    time.sleep(0.5)

# TODO(architkulkarni): how many connections and threads?
num_connections = int(num_remote_cpus * 0.75)
num_threads = 2
time_to_run = "10s"

pg = placement_group(
    [{"CPU: 1"} for _ in range(expected_num_nodes)], strategy="STRICT_SPREAD")
ray.get(pg.ready())


@ray.remote
def run_wrk():
    result = subprocess.run(
        [
            "wrk", "-c",
            str(num_connections), "-t",
            str(num_threads), "-s", time_to_run, "http://127.0.0.1:8000/hey"
        ],
        stdout=subprocess.PIPE)
    logger.info(result.stdout.decode())


run_wrk.options(placement_group=pg).remote()
