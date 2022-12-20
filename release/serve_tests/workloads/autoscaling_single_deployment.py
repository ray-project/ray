#!/usr/bin/env python3
"""
Benchmark test for single deployment at 1k no-op replica scale with
autoscaling.

1) Start with a single head node.
2) Autoscale up to 1k no-op replicas over N nodes.
3) Launch wrk in each running node to simulate load balanced request
5) Run a 10-minute wrk trial on each node, aggregate results.

Report:
    per_thread_latency_avg_ms
    per_thread_latency_max_ms
    per_thread_avg_tps
    per_thread_max_tps

    per_node_avg_tps
    per_node_avg_transfer_per_sec_KB

    cluster_total_thoughput
    cluster_total_transfer_KB
    cluster_max_P50_latency_ms
    cluster_max_P75_latency_ms
    cluster_max_P90_latency_ms
    cluster_max_P99_latency_ms
"""

import click
import json
import logging
import math
import os

from ray import serve
from serve_test_utils import (
    aggregate_all_metrics,
    run_wrk_on_all_nodes,
    save_test_results,
    is_smoke_test,
)
from serve_test_cluster_utils import (
    setup_local_single_node_cluster,
    setup_anyscale_cluster,
    warm_up_one_cluster,
    NUM_CPU_PER_NODE,
    NUM_CONNECTIONS,
)
from typing import Optional

logger = logging.getLogger(__file__)

# Experiment configs
DEFAULT_SMOKE_TEST_MIN_NUM_REPLICA = 0
DEFAULT_SMOKE_TEST_MAX_NUM_REPLICA = 4
DEFAULT_FULL_TEST_MIN_NUM_REPLICA = 0
DEFAULT_FULL_TEST_MAX_NUM_REPLICA = 1000

# Deployment configs
DEFAULT_MAX_BATCH_SIZE = 16

# Experiment configs - wrk specific
DEFAULT_SMOKE_TEST_TRIAL_LENGTH = "15s"
DEFAULT_FULL_TEST_TRIAL_LENGTH = "10m"


def deploy_replicas(min_replicas, max_replicas, max_batch_size):
    @serve.deployment(
        name="echo",
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": min_replicas,
            "max_replicas": max_replicas,
            "look_back_period_s": 0.2,
            "downscale_delay_s": 0.2,
            "upscale_delay_s": 0.2,
        },
        version="v1",
    )
    class Echo:
        @serve.batch(max_batch_size=max_batch_size)
        async def handle_batch(self, requests):
            return ["hi" for _ in range(len(requests))]

        async def __call__(self, request):
            return await self.handle_batch(request)

    Echo.deploy()


def save_results(final_result, default_name):
    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/single_deployment_1k_noop_replica.json"
    )
    with open(test_output_json, "wt") as f:
        json.dump(final_result, f)


@click.command()
@click.option("--min-replicas", "-min", type=int)
@click.option("--max-replicas", "-max", type=int)
@click.option("--trial-length", "-tl", type=str)
@click.option("--max-batch-size", type=int, default=DEFAULT_MAX_BATCH_SIZE)
def main(
    min_replicas: Optional[int],
    max_replicas: Optional[int],
    trial_length: Optional[str],
    max_batch_size: Optional[int],
):
    # Give default cluster parameter values based on smoke_test config
    # if user provided values explicitly, use them instead.
    # IS_SMOKE_TEST is set by args of releaser's e2e.py
    if is_smoke_test():
        min_replicas = min_replicas or DEFAULT_SMOKE_TEST_MIN_NUM_REPLICA
        max_replicas = max_replicas or DEFAULT_SMOKE_TEST_MAX_NUM_REPLICA
        trial_length = trial_length or DEFAULT_SMOKE_TEST_TRIAL_LENGTH
        logger.info(
            f"Running local / smoke test with min {min_replicas} and max "
            f"{max_replicas} replicas ..\n"
        )

        # Choose cluster setup based on user config. Local test uses Cluster()
        # to mock actors that requires # of nodes to be specified, but ray
        # client doesn't need to
        num_nodes = int(math.ceil(max_replicas / NUM_CPU_PER_NODE))
        logger.info(f"Setting up local ray cluster with {num_nodes} nodes ..\n")
        serve_client = setup_local_single_node_cluster(num_nodes)[0]
    else:
        min_replicas = min_replicas or DEFAULT_FULL_TEST_MIN_NUM_REPLICA
        max_replicas = max_replicas or DEFAULT_FULL_TEST_MAX_NUM_REPLICA
        trial_length = trial_length or DEFAULT_FULL_TEST_TRIAL_LENGTH
        logger.info(
            f"Running full test with min {min_replicas} and max "
            f"{max_replicas} replicas ..\n"
        )
        logger.info("Setting up anyscale ray cluster .. \n")
        serve_client = setup_anyscale_cluster()

    http_host = str(serve_client._http_config.host)
    http_port = str(serve_client._http_config.port)
    logger.info(f"Ray serve http_host: {http_host}, http_port: {http_port}")

    logger.info(
        f"Deploying with min {min_replicas} and max {max_replicas} "
        f"target replicas ....\n"
    )
    deploy_replicas(min_replicas, max_replicas, max_batch_size)

    logger.info("Warming up cluster ....\n")
    warm_up_one_cluster.remote(10, http_host, http_port, "echo")

    logger.info(f"Starting wrk trial on all nodes for {trial_length} ....\n")
    # For detailed discussion, see https://github.com/wg/wrk/issues/205
    # TODO:(jiaodong) What's the best number to use here ?
    all_endpoints = list(serve.list_deployments().keys())
    all_metrics, all_wrk_stdout = run_wrk_on_all_nodes(
        trial_length, NUM_CONNECTIONS, http_host, http_port, all_endpoints=all_endpoints
    )

    aggregated_metrics = aggregate_all_metrics(all_metrics)
    logger.info("Wrk stdout on each node: ")
    for wrk_stdout in all_wrk_stdout:
        logger.info(wrk_stdout)
    logger.info("Final aggregated metrics: ")
    for key, val in aggregated_metrics.items():
        logger.info(f"{key}: {val}")
    save_test_results(
        aggregated_metrics,
        default_output_file="/tmp/autoscaling_single_deployment.json",
    )


if __name__ == "__main__":
    main()
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
