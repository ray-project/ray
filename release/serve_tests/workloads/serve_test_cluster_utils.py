#!/usr/bin/env python3
import ray
import requests

from ray import serve
from ray.cluster_utils import Cluster
from ray.serve.utils import logger
from ray.serve.config import DeploymentMode

# Cluster setup configs
NUM_CPU_PER_NODE = 10
NUM_CONNECTIONS = 10


def setup_local_single_node_cluster(num_nodes):
    """Setup ray cluster locally via ray.init() and Cluster()

    Each actor is simulated in local process on single node,
    thus smaller scale by default.
    """
    cluster = Cluster()
    for i in range(num_nodes):
        cluster.add_node(
            redis_port=6379 if i == 0 else None,
            num_cpus=NUM_CPU_PER_NODE,
            num_gpus=0,
            resources={str(i): 2},
        )
    ray.init(address=cluster.address, dashboard_host="0.0.0.0")
    serve_client = serve.start(
        http_options={"location": DeploymentMode.EveryNode})

    return serve_client


def setup_anyscale_cluster():
    """Setup ray cluster at anyscale via ray.client()

    Note this is by default large scale and should be kicked off
    less frequently.
    """
    # TODO: Ray client didn't work with releaser script yet because
    # we cannot connect to anyscale cluster from its headnode
    # ray.client().env({}).connect()
    ray.init(address="auto")
    serve_client = serve.start(
        http_options={"location": DeploymentMode.EveryNode})

    return serve_client


@ray.remote
def warm_up_one_cluster(
        num_warmup_iterations: int,
        http_host: str,
        http_port: str,
        endpoint: str,
) -> None:
    logger.info(f"Warming up {endpoint} ..")
    for _ in range(num_warmup_iterations):
        resp = requests.get(f"http://{http_host}:{http_port}/{endpoint}").text
        logger.info(resp)
    return endpoint
