# A multi-node scalability test. We put an http proxy on the head node and spin
# up e.g. 20 nodes and put a bunch of replicas on the cluster, and run a stress
# test.

import logging
import time
import subprocess
import requests

import ray
from ray import serve
from ray.serve import BackendConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ray.init(address="auto")
client = serve.start()

# These numbers need to correspond with the autoscaler config file.
# The number of remote nodes in the autoscaler should upper bound
# these because sometimes nodes fail to update.
num_remote_nodes = 4
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


def hey(_):
    time.sleep(0.01)  # Sleep for 10ms
    return "hey"


# TODO(architkulkarni): how many replicas?
client.create_backend(
    "hey", hey, config=BackendConfig(num_replicas=num_remote_nodes / 2))
client.create_endpoint("hey", backend="hey", route="/hey")

logger.info("Warming up for ~3 seconds")
for _ in range(5):
    resp = requests.get("http://127.0.0.1:8000/hey").text
    logger.info("Received response \'" + resp + "\'")
    time.sleep(0.5)

num_connections = int(num_remote_cpus * 0.75)
num_threads = 2
time_to_run = "10s"

# TODO(architkulkarni): make long-running?
result = subprocess.run(
    [
        "wrk", "-c",
        str(num_connections), "-t",
        str(num_threads), "-s", time_to_run, "http://127.0.0.1:8000/hey"
    ],
    stdout=subprocess.PIPE)
logger.info(result.stdout.decode())
