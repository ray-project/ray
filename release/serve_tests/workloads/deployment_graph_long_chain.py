"""
Test that focuses on long chain of deployment graph
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
from ray.dag import InputNode
from ray.serve.drivers import DAGDriver
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
    def __init__(self, id: int, init_delay_secs=0):
        time.sleep(init_delay_secs)
        self.id = id

    async def compute(self, prev_val, compute_delay_secs=0):
        await asyncio.sleep(compute_delay_secs)

        return prev_val + 1


def test_long_chain_deployment_graph(
    chain_length, init_delay_secs=0, compute_delay_secs=0
):
    """
    Test that focuses on long chain of deployment graph
    INPUT -> Node_1 -> Node_2 -> ... -> Node_10 -> OUTPUT
    1) Intermediate blob size can be large / small
    2) Compute time each node can be long / short
    3) Init time can be long / short
    """

    nodes = [Node.bind(i, init_delay_secs=init_delay_secs) for i in range(chain_length)]
    prev_outputs = [None for _ in range(chain_length)]

    with InputNode() as user_input:
        for i in range(chain_length):
            if i == 0:
                prev_outputs[i] = nodes[i].compute.bind(
                    user_input, compute_delay_secs=compute_delay_secs
                )
            else:
                prev_outputs[i] = nodes[i].compute.bind(
                    prev_outputs[i - 1], compute_delay_secs=compute_delay_secs
                )

        serve_dag = DAGDriver.bind(prev_outputs[-1])

    return serve_dag


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
    assert ray.get(dag_handle.predict.remote(0)) == chain_length

    throughput_mean_tps, throughput_std_tps = asyncio.run(
        benchmark_throughput_tps(
            dag_handle,
            chain_length,
            duration_secs=throughput_trial_duration_secs,
            num_clients=num_clients,
        )
    )
    latency_mean_ms, latency_std_ms = asyncio.run(
        benchmark_latency_ms(
            dag_handle,
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
