"""
This test is parity of
release/serve_tests/workloads/deployment_graph_long_chain.py
Instead of using graph api, the test is using pure handle to
test long chain graph.

INPUT -> Node_1 -> Node_2 -> ... -> Node_10 -> OUTPUT

 1) Intermediate blob size can be large / small
 2) Compute time each node can be long / short
 3) Init time can be long / short
"""

import time
import asyncio
import click

from typing import Optional

import ray
from ray import serve
from ray.serve.context import get_global_client
from serve_test_cluster_utils import (
    setup_local_single_node_cluster,
    setup_anyscale_cluster,
)
from serve_test_utils import save_test_results
from benchmark_utils import benchmark_throughput_tps, benchmark_latency_ms

DEFAULT_CHAIN_LENGTH = 4

DEFAULT_NUM_REQUESTS_PER_CLIENT = 20  # request sent for latency test
DEFAULT_NUM_CLIENTS = 1  # Clients concurrently sending request to deployment

DEFAULT_THROUGHPUT_TRIAL_DURATION_SECS = 10


@serve.deployment
class Node:
    def __init__(
        self,
        id: int,
        prev_node=None,
        init_delay_secs=0,
        compute_delay_secs=0,
        sync_handle=True,
    ):
        time.sleep(init_delay_secs)
        self.id = id
        self.prev_node = prev_node
        self.compute_delay_secs = compute_delay_secs
        self.sync_handle = sync_handle

    async def predict(self, input_data: int):
        await asyncio.sleep(self.compute_delay_secs)
        if self.prev_node:
            if self.sync_handle:
                return await self.prev_node.predict.remote(input_data) + 1
            else:
                return await (await self.prev_node.predict.remote(input_data)) + 1
        else:
            return input_data + 1


def construct_long_chain_graph_with_pure_handle(
    chain_length, sync_handle: bool, init_delay_secs=0, compute_delay_secs=0
):
    prev_handle = None
    for id in range(chain_length):
        Node.options(name=str(id)).deploy(
            id, prev_handle, init_delay_secs, compute_delay_secs, sync_handle
        )
        prev_handle = get_global_client().get_handle(str(id), sync=sync_handle)
    return prev_handle


async def sanity_check_graph_deployment_with_async_handle(handle, expected_result):
    assert await (await handle.predict.remote(0)) == expected_result


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
@click.option("--sync-handle", type=bool, default=True)
def main(
    chain_length: Optional[int],
    init_delay_secs: Optional[int],
    compute_delay_secs: Optional[int],
    num_requests_per_client: Optional[int],
    num_clients: Optional[int],
    throughput_trial_duration_secs: Optional[int],
    local_test: Optional[bool],
    sync_handle: Optional[bool],
):
    if local_test:
        setup_local_single_node_cluster(1, num_cpu_per_node=8)
    else:
        setup_anyscale_cluster()

    handle = construct_long_chain_graph_with_pure_handle(
        chain_length,
        sync_handle,
        init_delay_secs=init_delay_secs,
        compute_delay_secs=compute_delay_secs,
    )
    if sync_handle:
        assert ray.get(handle.predict.remote(0)) == chain_length
    else:
        sanity_check_graph_deployment_with_async_handle(handle, chain_length)
    loop = asyncio.get_event_loop()

    throughput_mean_tps, throughput_std_tps = loop.run_until_complete(
        benchmark_throughput_tps(
            handle,
            chain_length,
            duration_secs=throughput_trial_duration_secs,
            num_clients=num_clients,
        )
    )
    latency_mean_ms, latency_std_ms = loop.run_until_complete(
        benchmark_latency_ms(
            handle,
            chain_length,
            num_requests=num_requests_per_client,
            num_clients=num_clients,
        )
    )

    print(f"chain_length: {chain_length}, num_clients: {num_clients}")
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
        "sync_handle": sync_handle,
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
            "perf_metric_name": "latency_std_ms",
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
