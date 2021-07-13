#!/usr/bin/env python3
"""
Benchmark test for single deployment at 1k no-op replica scale.

1) Start with a single head node.
2) Scale to 1k no-op replicas over N nodes.
3) Run 10 1-minute wrk trials on each node.

Report:
 - QPS
 - Latency (median, p90, p95, p99)
 - Time to reach 1k no-op running replicas
 - [P1] Memory delta / growth
"""

import click
import math
import ray
import requests
import subprocess
import time

from ray import serve
from ray.cluster_utils import Cluster
from subprocess import PIPE
from typing import Optional
from serve_test_utils import parse_wrk_decoded_stdout

# Experiment configs
DEFAULT_NUM_REPLICA = 8
DEFAULT_NUM_TRIALS = 1

# Cluster setup configs
NUM_REDIS_SHARDS = 1
REDIS_MAX_MEMORY = 10**8
OBJECT_STORE_MEMORY = 10**8
NUM_CPU_PER_NODE = 8

# Deployment configs
DEFAULT_MAX_BATCH_SIZE = 16

# Experiment configs - wrk specific
DEFAULT_TRAIL_LENGTH = "1m"
# For detailed discussion, see https://github.com/wg/wrk/issues/205
# TODO:(jiaodong) What's the best number to use here ?
DEFAULT_NUM_CONNECTIONS = int(
    DEFAULT_NUM_REPLICA * DEFAULT_MAX_BATCH_SIZE * 0.75)


def setup_cluster(num_nodes):
    cluster = Cluster()
    for i in range(num_nodes):
        cluster.add_node(
            redis_port=6379 if i == 0 else None,
            num_redis_shards=NUM_REDIS_SHARDS if i == 0 else None,
            num_cpus=NUM_CPU_PER_NODE,
            num_gpus=0,
            resources={str(i): 2},
            object_store_memory=OBJECT_STORE_MEMORY,
            redis_max_memory=REDIS_MAX_MEMORY,
            dashboard_host="0.0.0.0",
        )
    ray.init(address=cluster.address, dashboard_host="0.0.0.0")
    serve.start()


def deploy_replicas(num_replicas, max_batch_size):
    @serve.deployment(name="echo", num_replicas=num_replicas)
    class Echo:
        @serve.batch(max_batch_size=max_batch_size)
        async def handle_batch(self, requests):
            return ["hi" for _ in range(len(requests))]

        async def __call__(self, request):
            return await self.handle_batch(request)

    Echo.deploy()


def warm_up_cluster(num_warmup_iterations: int) -> None:
    for _ in range(num_warmup_iterations):
        resp = requests.get("http://127.0.0.1:8000/echo").text
        print(resp)
        time.sleep(0.5)


def run_one_trial(trail_length: str, num_connectionss) -> None:
    proc = subprocess.Popen(
        [
            "wrk",
            "-c",
            str(num_connectionss),
            "-t",
            str(NUM_CPU_PER_NODE),
            "-d",
            trail_length,
            "http://127.0.0.1:8000/echo",
        ],
        stdout=PIPE,
        stderr=PIPE,
    )
    proc.wait()
    out, err = proc.communicate()
    print(out.decode())
    print(err.decode())

    return out.decode()


def shutdown_cluster():
    pass


@click.command()
@click.option("--num-replicas", type=int, default=DEFAULT_NUM_REPLICA)
@click.option("--num-trials", type=int, default=DEFAULT_NUM_TRIALS)
@click.option("--trial-length", type=str, default=DEFAULT_TRAIL_LENGTH)
@click.option("--max-batch-size", type=int, default=DEFAULT_MAX_BATCH_SIZE)
def main(num_replicas: Optional[int], num_trials: Optional[int],
         trial_length: Optional[str], max_batch_size: Optional[int]):
    num_nodes = int(math.ceil(DEFAULT_NUM_REPLICA / NUM_CPU_PER_NODE))
    print(f"\n\nSetting up ray cluster with {num_nodes} nodes ....\n\n")
    setup_cluster(num_nodes)

    print(f"\n\nDeploying with {num_replicas} target replicas ....\n\n")
    deploy_replicas(num_replicas, max_batch_size)

    print("\n\nWarming up cluster ....\n\n")
    warm_up_cluster(5)

    final_result = []
    for iteration in range(num_trials):
        print(f"\n\nStarting wrk trail # {iteration + 1} ....\n\n")
        decoded_out = run_one_trial(trial_length, DEFAULT_NUM_CONNECTIONS)
        metrics_dict = parse_wrk_decoded_stdout(decoded_out)
        final_result.append(metrics_dict)

    print(final_result)


if __name__ == "__main__":
    main()
