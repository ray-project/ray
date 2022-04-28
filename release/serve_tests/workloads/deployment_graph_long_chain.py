"""
Test that focuses on long chain of deployment graph

INPUT -> Node_1 -> Node_2 -> ... -> Node_20 -> OUTPUT

 1) Intermediate blob size can be large / small
 2) Compute time each node can be long / short
 3) Init time can be long / short
 4) Validate performance between Ray DAG and Serve DAG with 1 replica.

"""
import time
import asyncio
import click

import numpy as np
from typing import Optional

import ray
from ray import serve
from ray.experimental.dag import InputNode
from ray.serve.drivers import DAGDriver
from serve_test_cluster_utils import (
    setup_local_single_node_cluster,
    setup_anyscale_cluster,
)
from serve_test_utils import save_test_results
from benchmark_utils import (
    DeploymentHandleClient,
    measure_latency_ms,
    measure_throughput_tps,
)

DEFAULT_CHAIN_LENGTH = 4

DEFAULT_NUM_REQUESTS_PER_CLIENT = 20  # request sent for latency test
DEFAULT_NUM_CLIENTS = 1  # Clients concurrently sending request to deployment

DEFAULT_THROUGHPUT_TRIAL_DURATION_SECS = 10


@serve.deployment
class Node:
    def __init__(self, id: int, prev_node=None, init_delay_secs=0):
        time.sleep(init_delay_secs)
        self.id = id
        self.prev_node = prev_node
        return

    async def compute(self, input_data, compute_delay_secs=0):
        await asyncio.sleep(compute_delay_secs)

        if self.prev_node:
            return await self.prev_node.compute.remote(input_data) + 1
        else:
            return input_data + 1


def test_long_chain_deployment_graph(
    chain_length, init_delay_secs=0, compute_delay_secs=0
):
    with InputNode() as user_input:
        prev_output = user_input
        prev_node = None
        for i in range(chain_length):
            node = Node.bind(i, prev_node=prev_node, init_delay_secs=init_delay_secs)
            node_output = node.compute.bind(
                prev_output, compute_delay_secs=compute_delay_secs
            )

            prev_output = node_output
            prev_node = node

        serve_dag = DAGDriver.bind(prev_output)

    return serve_dag


async def benchmark_throughput_tps(
    dag_handle,
    expected,
    duration_secs=DEFAULT_THROUGHPUT_TRIAL_DURATION_SECS,
    num_clients=DEFAULT_NUM_CLIENTS,
):
    """Call deployment handle in a blocking for loop."""
    clients = [
        DeploymentHandleClient.remote(measure_throughput_tps)
        for _ in range(num_clients)
    ]
    ray.get([client.ready.remote() for client in clients])

    throughput_stats_tps_list = ray.get(
        [
            a.run.remote(
                dag_handle.predict.remote,
                0,
                expected,
                duration_secs=duration_secs,
            )
            for a in clients
        ]
    )
    throughput_stats_tps = []
    for client_rst in throughput_stats_tps_list:
        throughput_stats_tps.extend(client_rst)

    mean = round(np.mean(throughput_stats_tps), 2)
    std = round(np.std(throughput_stats_tps), 2)
    return mean, std


async def benchmark_latency_ms(
    dag_handle, expected, num_requests=100, num_clients=DEFAULT_NUM_CLIENTS
):
    """Call deployment handle in a blocking for loop."""
    clients = [
        DeploymentHandleClient.remote(measure_latency_ms) for _ in range(num_clients)
    ]
    ray.get([client.ready.remote() for client in clients])

    latency_stats_ms_list = ray.get(
        [
            a.run.remote(
                dag_handle.predict.remote,
                0,
                expected,
                num_requests=num_requests,
            )
            for a in clients
        ]
    )
    latency_stats_ms = []
    for client_rst in latency_stats_ms_list:
        latency_stats_ms.extend(client_rst)

    mean = round(np.mean(latency_stats_ms), 2)
    std = round(np.std(latency_stats_ms), 2)
    return mean, std


@click.command()
@click.option("--chain-length", type=int, default=DEFAULT_CHAIN_LENGTH)
@click.option("--init-delay-secs", type=int, default=0)
@click.option("--compute-delay-secs", type=int, default=0)
@click.option(
    "--num-requests-per-client",
    type=int,
    default=DEFAULT_NUM_REQUESTS_PER_CLIENT,
)
@click.option("--num-clients", type=int, default=DEFAULT_NUM_CLIENTS)
@click.option(
    "--throughput-trial-duration-secs",
    type=int,
    default=DEFAULT_THROUGHPUT_TRIAL_DURATION_SECS,
)
@click.option("--local-test", type=bool, default=True)
def main(
    chain_length: Optional[int],
    init_delay_secs: Optional[int],
    compute_delay_secs: Optional[int],
    num_requests_per_client: Optional[int],
    num_clients: Optional[int],
    throughput_trial_duration_secs: Optional[int],
    local_test: Optional[bool],
):
    if local_test:
        setup_local_single_node_cluster(1, num_cpu_per_node=8)
    else:
        setup_anyscale_cluster()

    serve_dag = test_long_chain_deployment_graph(
        chain_length,
        init_delay_secs=init_delay_secs,
        compute_delay_secs=compute_delay_secs,
    )
    dag_handle = serve.run(serve_dag)

    # 1 + 2 + 3 + 4 + ... + chain_length
    expected = ((1 + chain_length) * chain_length) / 2
    assert ray.get(dag_handle.predict.remote(0)) == expected
    loop = asyncio.get_event_loop()

    throughput_mean_tps, throughput_std_tps = loop.run_until_complete(
        benchmark_throughput_tps(
            dag_handle,
            expected,
            duration_secs=throughput_trial_duration_secs,
            num_clients=num_clients,
        )
    )
    latency_mean_ms, latency_std_ms = loop.run_until_complete(
        benchmark_latency_ms(
            dag_handle,
            expected,
            num_requests=num_requests_per_client,
            num_clients=num_clients,
        )
    )
    print(f"latency_mean_ms: {latency_mean_ms}, " f"latency_std_ms: {latency_std_ms}")
    print(
        f"throughput_mean_tps: {throughput_mean_tps}, "
        f"throughput_std_tps: {throughput_std_tps}"
    )

    results = {
        "chain_length": chain_length,
        "init_delay_secs": init_delay_secs,
        "compute_delay_secs": compute_delay_secs,
        "local_test": local_test,
    }
    results["perf_metrics"] = [
        {
            "perf_metric_name": "throughput_mean_tps",
            "perf_metric_value": throughput_mean_tps,
            "perf_metric_type": "THROUGHPUT",
        },
        {
            "perf_metric_name": "throughput_std_tps",
            "perf_metric_value": throughput_std_tps,
            "perf_metric_type": "THROUGHPUT",
        },
        {
            "perf_metric_name": "latency_mean_ms",
            "perf_metric_value": latency_mean_ms,
            "perf_metric_type": "LATENCY",
        },
        {
            "perf_metric_name": "requests_std_ms",
            "perf_metric_value": latency_std_ms,
            "perf_metric_type": "LATENCY",
        },
    ]
    save_test_results(results)


if __name__ == "__main__":
    main()
    import sys
    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
