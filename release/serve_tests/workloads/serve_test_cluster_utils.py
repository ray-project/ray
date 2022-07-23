#!/usr/bin/env python3
import logging

import ray
import requests
from ray import serve
from ray._private.test_utils import monitor_memory_usage
from ray.cluster_utils import Cluster
from ray.serve.config import DeploymentMode
from ray.serve.constants import DEFAULT_CHECKPOINT_PATH

logger = logging.getLogger(__file__)

# Cluster setup configs
NUM_CPU_PER_NODE = 10
NUM_CONNECTIONS = 10


def setup_local_single_node_cluster(
    num_nodes: int,
    num_cpu_per_node=NUM_CPU_PER_NODE,
    checkpoint_path: str = DEFAULT_CHECKPOINT_PATH,
    namespace="serve",
):
    """Setup ray cluster locally via ray.init() and Cluster()

    Each actor is simulated in local process on single node,
    thus smaller scale by default.
    """
    cluster = Cluster()
    for i in range(num_nodes):
        cluster.add_node(
            redis_port=6380 if i == 0 else None,
            num_cpus=num_cpu_per_node,
            num_gpus=0,
            resources={str(i): 2},
        )
    ray.init(address=cluster.address, dashboard_host="0.0.0.0", namespace=namespace)
    serve_client = serve.start(
        detached=True,
        http_options={"location": DeploymentMode.EveryNode},
        _checkpoint_path=checkpoint_path,
    )

    return serve_client, cluster


def setup_anyscale_cluster(checkpoint_path: str = DEFAULT_CHECKPOINT_PATH):
    """Setup ray cluster at anyscale via ray.client()

    Note this is by default large scale and should be kicked off
    less frequently.
    """
    # TODO: Ray client didn't work with releaser script yet because
    # we cannot connect to anyscale cluster from its headnode
    # ray.client().env({}).connect()
    ray.init(
        address="auto",
        # This flag can be enabled to debug node autoscaler events.
        # But the cluster scaling has been stable for now, so we turn it off
        # to reduce spam.
        runtime_env={"env_vars": {"SERVE_ENABLE_SCALING_LOG": "0"}},
    )
    serve_client = serve.start(
        http_options={"location": DeploymentMode.EveryNode},
        _checkpoint_path=checkpoint_path,
    )

    # Print memory usage on the head node to help diagnose/debug memory leaks.
    monitor_memory_usage()

    return serve_client


@ray.remote
def warm_up_one_cluster(
    num_warmup_iterations: int,
    http_host: str,
    http_port: str,
    endpoint: str,
    nonblocking: bool = False,
) -> None:
    # Specifying a low timeout effectively makes requests.get() nonblocking
    timeout = 0.0001 if nonblocking else None
    logger.info(f"Warming up {endpoint} ..")
    for _ in range(num_warmup_iterations):
        try:
            resp = requests.get(
                f"http://{http_host}:{http_port}/{endpoint}", timeout=timeout
            ).text
            logger.info(resp)
        except requests.exceptions.ReadTimeout:
            # This exception only gets raised if a timeout is specified in the
            # requests.get() call.
            logger.info("Issued nonblocking HTTP request.")

    return endpoint
