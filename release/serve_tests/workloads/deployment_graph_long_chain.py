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
import pytest
import sys
import logging
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

logger = logging.getLogger(__file__)

# @serve.deployment
# class DAGDriver:
#     def __init__(self, dag_handle):
#         self.dag_handle = dag_handle

#     async def predict(self, inp):
#         """Perform inference directly without HTTP."""
#         return await self.dag_handle.remote(inp)

#     async def __call__(self, request):
#         """HTTP endpoint of the DAG."""
#         input_data = await request.json()
#         return await self.predict(input_data)


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
    chain_length=20, init_delay_secs=0, compute_delay_secs=0
):
    with InputNode() as user_input:
        prev_output = user_input
        prev_node = None
        for i in range(chain_length):
            node = Node.bind(
                i, prev_node=prev_node, init_delay_secs=init_delay_secs
            )
            node_output = node.compute.bind(
                prev_output, compute_delay_secs=compute_delay_secs
            )

            prev_output = node_output
            prev_node = node

        serve_dag = DAGDriver.bind(prev_output)

    return serve_dag

async def timeit(name, fn, multiplier=1):
    # warmup
    start = time.time()
    while time.time() - start < 1:
        await fn()
    # real run
    stats = []
    for _ in range(4):
        start = time.time()
        count = 0
        while time.time() - start < 2:
            await fn()
            count += 1
        end = time.time()
        stats.append(multiplier * count / (end - start))
    logger.info(
        "\t{} {} +- {} requests/s".format(
            name, round(np.mean(stats), 2), round(np.std(stats), 2)
        )
    )
    return round(np.mean(stats), 2)

async def benchmark(dag_handle, num_calls=10):
    async def run():
        for _ in range(num_calls):
            await dag_handle.predict.remote(0)

    return await timeit("Single client long chain", run, multiplier=1)


@click.command()
@click.option("--chain-length", type=int, default=4)
@click.option("--init-delay-secs", type=int, default=0)
@click.option("--compute-delay-secs", type=int, default=0)
@click.option("--local-test", type=bool, default=True)
def main(
    chain_length: Optional[int],
    init_delay_secs: Optional[int],
    compute_delay_secs: Optional[int],
    local_test: Optional[bool],
):
    if local_test:
        setup_local_single_node_cluster(4, num_cpu_per_node=2)
        serve_dag = test_long_chain_deployment_graph(
            chain_length=chain_length,
            init_delay_secs=init_delay_secs,
            compute_delay_secs=compute_delay_secs,
        )
        dag_handle = serve.run(serve_dag)

        # 1 + 2 + 3 + 4 + ... chain_length
        assert (
            ray.get(dag_handle.predict.remote(0))
            == ((1 + chain_length) * chain_length) / 2
        )
        loop = asyncio.get_event_loop()
        stats = loop.run_until_complete(benchmark(dag_handle))
        logger.info(f"\n\n\n{stats}\n\n\n")
    else:
        setup_anyscale_cluster()
        serve_dag = test_long_chain_deployment_graph(
            chain_length=chain_length,
            init_delay_secs=init_delay_secs,
            compute_delay_secs=compute_delay_secs,
        )
        dag_handle = serve.run(serve_dag)


if __name__ == "__main__":
    main()

    sys.exit(pytest.main(["-v", "-s", __file__]))
