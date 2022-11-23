"""
Test that focuses on wide fanout of deployment graph
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

DEFAULT_FANOUT_DEGREE = 4

DEFAULT_NUM_REQUESTS_PER_CLIENT = 20  # request sent for latency test
DEFAULT_NUM_CLIENTS = 1  # Clients concurrently sending request to deployment

DEFAULT_THROUGHPUT_TRIAL_DURATION_SECS = 10


@serve.deployment
class Node:
    def __init__(self, id: int, init_delay_secs=0):
        time.sleep(init_delay_secs)
        self.id = id

    async def compute(self, input_data, compute_delay_secs=0):
        await asyncio.sleep(compute_delay_secs)

        return input_data + self.id


@serve.deployment
def combine(value_refs):
    return sum(ray.get(value_refs))


def test_wide_fanout_deployment_graph(
    fanout_degree, init_delay_secs=0, compute_delay_secs=0
):
    """
    Test that focuses on wide fanout of deployment graph
        -> Node_1
        /          \
    INPUT --> Node_2  --> combine -> OUTPUT
        \    ...   /
        -> Node_10

    1) Intermediate blob size can be large / small
    2) Compute time each node can be long / short
    3) Init time can be long / short
    """
    nodes = [
        Node.bind(i, init_delay_secs=init_delay_secs) for i in range(0, fanout_degree)
    ]
    outputs = []
    with InputNode() as user_input:
        for i in range(0, fanout_degree):
            outputs.append(
                nodes[i].compute.bind(user_input, compute_delay_secs=compute_delay_secs)
            )

        dag = combine.bind(outputs)

        serve_dag = DAGDriver.bind(dag)

    return serve_dag


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
def main(
    fanout_degree: Optional[int],
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

    serve_dag = test_wide_fanout_deployment_graph(
        fanout_degree,
        init_delay_secs=init_delay_secs,
        compute_delay_secs=compute_delay_secs,
    )
    dag_handle = serve.run(serve_dag)

    # 0 + 1 + 2 + 3 + 4 + ... + (fanout_degree - 1)
    expected = ((0 + fanout_degree - 1) * fanout_degree) / 2
    assert ray.get(dag_handle.predict.remote(0)) == expected

    throughput_mean_tps, throughput_std_tps = asyncio.run(
        benchmark_throughput_tps(
            dag_handle,
            expected,
            duration_secs=throughput_trial_duration_secs,
            num_clients=num_clients,
        )
    )
    latency_mean_ms, latency_std_ms = asyncio.run(
        benchmark_latency_ms(
            dag_handle,
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
