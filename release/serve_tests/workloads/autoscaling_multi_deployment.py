#!/usr/bin/env python3
"""
Benchmark test for multi deployment with autoscaling at up to 1k no-op replica
scale.

1) Start with a single head node.
2) Start 10 deployments each with up to 100 no-op replicas
3) Launch wrk in each running node to simulate load balanced request
4) Recursively send queries to random deployments, up to depth=5
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
import math
import os
import random

import ray
from ray import serve
from ray.serve.utils import logger
from serve_test_utils import (
    aggregate_all_metrics,
    run_wrk_on_all_nodes,
    save_test_results,
)
from serve_test_cluster_utils import (
    setup_local_single_node_cluster,
    setup_anyscale_cluster,
    warm_up_one_cluster,
    NUM_CPU_PER_NODE,
    NUM_CONNECTIONS,
)
from typing import Optional

# Experiment configs
DEFAULT_SMOKE_TEST_MIN_NUM_REPLICA = 1
DEFAULT_SMOKE_TEST_MAX_NUM_REPLICA = 8
DEFAULT_SMOKE_TEST_NUM_DEPLOYMENTS = 4  # 2 replicas each

# TODO:(jiaodong) We should investigate and change this back to 1k
# for now, we won't get valid latency numbers from wrk at 1k replica
# likely due to request timeout.
DEFAULT_FULL_TEST_MIN_NUM_REPLICA = 1
DEFAULT_FULL_TEST_MAX_NUM_REPLICA = 1000
# TODO(simon): we should change this back to 100. But due to long poll issue
# we temporarily downscoped this test.
# https://github.com/ray-project/ray/pull/20270
DEFAULT_FULL_TEST_NUM_DEPLOYMENTS = 10  # 100 replicas each

# Experiment configs - wrk specific
DEFAULT_SMOKE_TEST_TRIAL_LENGTH = "15s"
DEFAULT_FULL_TEST_TRIAL_LENGTH = "10m"


def setup_multi_deployment_replicas(min_replicas, max_replicas,
                                    num_deployments):
    max_replicas_per_deployment = max_replicas // num_deployments
    all_deployment_names = [f"Echo_{i+1}" for i in range(num_deployments)]

    @serve.deployment(
        _autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": min_replicas,
            "max_replicas": max_replicas_per_deployment,
            "look_back_period_s": 0.2,
            "downscale_delay_s": 0.2,
            "upscale_delay_s": 0.2
        },
        version="v1")
    class Echo:
        def __init__(self):
            self.all_deployment_async_handles = []

        def get_random_async_handle(self):
            # sync get_handle() and expected to be called only a few times
            # during deployment warmup so each deployment has reference to
            # all other handles to send recursive inference call
            if len(self.all_deployment_async_handles) < len(
                    all_deployment_names):
                deployments = list(serve.list_deployments().values())
                self.all_deployment_async_handles = [
                    deployment.get_handle(sync=False)
                    for deployment in deployments
                ]

            return random.choice(self.all_deployment_async_handles)

        async def handle_request(self, request, depth: int):
            # Max recursive call depth reached
            if depth > 4:
                return "hi"

            next_async_handle = self.get_random_async_handle()
            obj_ref = await next_async_handle.handle_request.remote(
                request, depth + 1)

            return await obj_ref

        async def __call__(self, request):
            return await self.handle_request(request, 0)

    for deployment in all_deployment_names:
        Echo.options(name=deployment).deploy()


@click.command()
@click.option("--min-replicas", "-min", type=int)
@click.option("--max-replicas", "-max", type=int)
@click.option("--num-deployments", "-nd", type=int)
@click.option("--trial-length", "-tl", type=str)
def main(min_replicas: Optional[int], max_replicas: Optional[int],
         num_deployments: Optional[int], trial_length: Optional[str]):
    # Give default cluster parameter values based on smoke_test config
    # if user provided values explicitly, use them instead.
    # IS_SMOKE_TEST is set by args of releaser's e2e.py
    smoke_test = os.environ.get("IS_SMOKE_TEST", "1")
    if smoke_test == "1":
        min_replicas = min_replicas or DEFAULT_SMOKE_TEST_MIN_NUM_REPLICA
        max_replicas = max_replicas or DEFAULT_SMOKE_TEST_MAX_NUM_REPLICA
        num_deployments = num_deployments or DEFAULT_SMOKE_TEST_NUM_DEPLOYMENTS
        trial_length = trial_length or DEFAULT_SMOKE_TEST_TRIAL_LENGTH
        logger.info(f"Running smoke test with min {min_replicas} and max "
                    f"{max_replicas} replicas, {num_deployments} deployments "
                    f".. \n")
        # Choose cluster setup based on user config. Local test uses Cluster()
        # to mock actors that requires # of nodes to be specified, but ray
        # client doesn't need to
        num_nodes = int(math.ceil(max_replicas / NUM_CPU_PER_NODE))
        logger.info(
            f"Setting up local ray cluster with {num_nodes} nodes .. \n")
        serve_client = setup_local_single_node_cluster(num_nodes)[0]
    else:
        min_replicas = min_replicas or DEFAULT_FULL_TEST_MIN_NUM_REPLICA
        max_replicas = max_replicas or DEFAULT_FULL_TEST_MAX_NUM_REPLICA
        num_deployments = num_deployments or DEFAULT_FULL_TEST_NUM_DEPLOYMENTS
        trial_length = trial_length or DEFAULT_FULL_TEST_TRIAL_LENGTH
        logger.info(f"Running full test with min {min_replicas} and max "
                    f"{max_replicas} replicas, {num_deployments} deployments "
                    f".. \n")
        logger.info("Setting up anyscale ray cluster .. \n")
        serve_client = setup_anyscale_cluster()

    http_host = str(serve_client._http_config.host)
    http_port = str(serve_client._http_config.port)
    logger.info(f"Ray serve http_host: {http_host}, http_port: {http_port}")

    logger.info(f"Deploying with min {min_replicas} and max {max_replicas}"
                f"target replicas ....\n")
    setup_multi_deployment_replicas(min_replicas, max_replicas,
                                    num_deployments)

    logger.info("Warming up cluster ....\n")
    endpoint_refs = []
    all_endpoints = list(serve.list_deployments().keys())
    for endpoint in all_endpoints:
        endpoint_refs.append(
            warm_up_one_cluster.options(num_cpus=0).remote(
                10, http_host, http_port, endpoint))
    for endpoint in ray.get(endpoint_refs):
        logger.info(f"Finished warming up {endpoint}")

    logger.info(f"Starting wrk trial on all nodes for {trial_length} ....\n")
    # For detailed discussion, see https://github.com/wg/wrk/issues/205
    # TODO:(jiaodong) What's the best number to use here ?
    all_metrics, all_wrk_stdout = run_wrk_on_all_nodes(
        trial_length,
        NUM_CONNECTIONS,
        http_host,
        http_port,
        all_endpoints=all_endpoints)

    aggregated_metrics = aggregate_all_metrics(all_metrics)
    logger.info("Wrk stdout on each node: ")
    for wrk_stdout in all_wrk_stdout:
        logger.info(wrk_stdout)
    logger.info("Final aggregated metrics: ")
    for key, val in aggregated_metrics.items():
        logger.info(f"{key}: {val}")
    save_test_results(
        aggregated_metrics,
        default_output_file="/tmp/autoscaling_multi_deployment.json")


if __name__ == "__main__":
    main()
    import pytest
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
