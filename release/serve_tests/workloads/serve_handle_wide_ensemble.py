r"""
This test is parity of
release/serve_tests/workloads/deployment_graph_wide_ensemble.py
Instead of using graph api, the test is using pure handle to
test wide fanout graph


       -> Node_1
      /          \
INPUT --> Node_2  --> combine -> OUTPUT
      \    ...   /
       -> Node_10

 1) Intermediate blob size can be large / small
 2) Compute time each node can be long / short
 3) Init time can be long / short
"""

import time
import asyncio
import click

from typing import List
from typing import Optional

import ray
from ray import serve
from ray.serve.handle import RayServeSyncHandle
from ray.serve.context import get_global_client
from serve_test_cluster_utils import (
    setup_local_single_node_cluster,
    setup_anyscale_cluster,
)
from serve_test_utils import save_test_results
from benchmark_utils import benchmark_throughput_tps, benchmark_latency_ms

DEFAULT_FANOUT_DEGREE = 4

DEFAULT_NUM_REQUESTS_PER_CLIENT = 20  # request sent for latency test
DEFAULT_NUM_CLIENTS = 1  # Clients concurrently sending request to deployment

DEFAULT_THROUGHPUT_TRIAL_DURATION_SECS = 10


@serve.deployment
class Node:
    def __init__(self, id: int, init_delay_secs=0):
        time.sleep(init_delay_secs)
        self.id = id

    async def predict(self, input_data, compute_delay_secs=0):
        await asyncio.sleep(compute_delay_secs)

        return input_data + self.id


@serve.deployment
class CombineNode:
    def __init__(
        self,
        input_nodes: List[RayServeSyncHandle],
        compute_delay_secs,
        sync_handle=True,
    ):
        assert type(input_nodes) == list
        self.input_nodes = input_nodes
        self.compute_delay_secs = compute_delay_secs
        self.sync_handle = sync_handle

    async def predict(self, data):
        results = [
            node.predict.remote(data, self.compute_delay_secs)
            for node in self.input_nodes
        ]
        if self.sync_handle:
            results = await asyncio.gather(*results)
        else:
            results = await asyncio.gather(*await asyncio.gather(*results))
        return sum(results)


def construct_wide_fanout_graph_with_pure_handle(
    fanout_degree, sync_handle: bool, init_delay_secs=0, compute_delay_secs=0
) -> RayServeSyncHandle:
    nodes = []
    for id in range(fanout_degree):
        Node.options(name=str(id)).deploy(id, init_delay_secs=init_delay_secs)
        nodes.append(get_global_client().get_handle(str(id), sync=sync_handle))
    CombineNode.options(name="combine").deploy(
        nodes, compute_delay_secs, sync_handle=sync_handle
    )
    return get_global_client().get_handle("combine", sync=sync_handle)


async def sanity_check_graph_deployment_with_async_handle(handle, expected_result):
    assert await (await handle.predict.remote(0)) == expected_result


@click.command()
@click.option("--fanout-degree", type=int, default=DEFAULT_FANOUT_DEGREE)
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
    fanout_degree: Optional[int],
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

    handle = construct_wide_fanout_graph_with_pure_handle(
        fanout_degree,
        sync_handle,
        init_delay_secs=init_delay_secs,
        compute_delay_secs=compute_delay_secs,
    )

    # 0 + 1 + 2 + 3 + 4 + ... + (fanout_degree - 1)
    expected = ((0 + fanout_degree - 1) * fanout_degree) / 2
    if sync_handle:
        assert ray.get(handle.predict.remote(0)) == expected
    else:
        sanity_check_graph_deployment_with_async_handle(handle, expected)

    throughput_mean_tps, throughput_std_tps = asyncio.run(
        benchmark_throughput_tps(
            handle,
            expected,
            duration_secs=throughput_trial_duration_secs,
            num_clients=num_clients,
        )
    )
    latency_mean_ms, latency_std_ms = asyncio.run(
        benchmark_latency_ms(
            handle,
            expected,
            num_requests=num_requests_per_client,
            num_clients=num_clients,
        )
    )
    print(f"fanout_degree: {fanout_degree}, num_clients: {num_clients}")
    print(f"latency_mean_ms: {latency_mean_ms}, " f"latency_std_ms: {latency_std_ms}")
    print(
        f"throughput_mean_tps: {throughput_mean_tps}, "
        f"throughput_std_tps: {throughput_std_tps}"
    )

    results = {
        "fanout_degree": fanout_degree,
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
