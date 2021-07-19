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
import json
import math
import os
import ray
import requests
import subprocess
import time

from ray import serve
from ray.cluster_utils import Cluster
from serve_test_utils import parse_wrk_decoded_stdout
from subprocess import PIPE
from typing import Optional

# Experiment configs
DEFAULT_SMOKE_TEST_NUM_REPLICA = 8
DEFAULT_SMOKE_TEST_NUM_TRIALS = 1

DEFAULT_FULL_TEST_NUM_REPLICA = 1000
DEFAULT_FULL_TEST_NUM_TRIALS = 10

# Cluster setup configs
NUM_REDIS_SHARDS = 1
REDIS_MAX_MEMORY = 10**8
OBJECT_STORE_MEMORY = 10**8
NUM_CPU_PER_NODE = 8

# Deployment configs
DEFAULT_MAX_BATCH_SIZE = 16

# Experiment configs - wrk specific
DEFAULT_SMOKE_TEST_TRIAL_LENGTH = "10s"
DEFAULT_FULL_TEST_TRIAL_LENGTH = "10m"


def setup_local_single_node_cluster(num_nodes):
    """Setup ray cluster locally via ray.init() and Cluster()

    Each actor is simulated in local process on single node,
    thus smaller scale by default.
    """
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


def setup_anyscale_cluster():
    """Setup ray cluster at anyscale via ray.client()

    Note this is by default large scale and should be kicked off
    less frequently.
    """
    # TODO: Ray client didn't work with releaser script yet because
    # we cannot connect to anyscale cluster from its headnode
    # ray.client().env({}).connect()
    ray.init(address="auto")
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


def run_one_trial(trial_length: str, num_connectionss) -> None:
    proc = subprocess.Popen(
        [
            "wrk",
            "-c",
            str(num_connectionss),
            "-t",
            str(NUM_CPU_PER_NODE),
            "-d",
            trial_length,
            "--latency",
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
@click.option("--num-replicas", type=int)
@click.option("--num-trials", type=int)
@click.option("--trial-length", type=str)
@click.option("--max-batch-size", type=int, default=DEFAULT_MAX_BATCH_SIZE)
@click.option("--run-locally", type=bool, default=True)
def main(num_replicas: Optional[int], num_trials: Optional[int],
         trial_length: Optional[str], max_batch_size: Optional[int],
         run_locally: Optional[bool]):

    # Give default cluster parameter values based on smoke_test config
    # if user provided values explicitly, use them instead.
    # IS_SMOKE_TEST is set by args of releaser's e2e.py
    smoke_test = os.environ.get("IS_SMOKE_TEST", "0")
    if smoke_test == "0":
        num_replicas = num_replicas or DEFAULT_FULL_TEST_NUM_REPLICA
        num_trials = num_trials or DEFAULT_FULL_TEST_NUM_TRIALS
        trial_length = trial_length or DEFAULT_FULL_TEST_TRIAL_LENGTH
        print(f"\nRunning full test with {num_replicas} replicas, "
              f"{num_trials} trials that lasts {trial_length} each.. \n")
    else:
        num_replicas = num_replicas or DEFAULT_SMOKE_TEST_NUM_REPLICA
        num_trials = num_trials or DEFAULT_SMOKE_TEST_NUM_TRIALS
        trial_length = trial_length or DEFAULT_SMOKE_TEST_TRIAL_LENGTH
        print(f"\nRunning smoke test with {num_replicas} replicas, "
              f"{num_trials} trials that lasts {trial_length} each.. \n")

    # Choose cluster setup based on user config. Local test uses Cluster()
    # to mock actors that requires # of nodes to be specified, but ray
    # client doesn't need to
    if run_locally:
        num_nodes = int(math.ceil(num_replicas / NUM_CPU_PER_NODE))
        print(f"\nSetting up local ray cluster with {num_nodes} nodes ....\n")
        setup_local_single_node_cluster(num_nodes)
    else:
        print("\nSetting up anyscale ray cluster .. \n")
        setup_anyscale_cluster()

    print(f"\nDeploying with {num_replicas} target replicas ....\n")
    deploy_replicas(num_replicas, max_batch_size)

    print("\nWarming up cluster ....\n")
    warm_up_cluster(5)

    final_result = []
    for iteration in range(num_trials):
        print(f"\nStarting wrk trial # {iteration + 1} ....\n")
        # For detailed discussion, see https://github.com/wg/wrk/issues/205
        # TODO:(jiaodong) What's the best number to use here ?
        num_connections = int(num_replicas * DEFAULT_MAX_BATCH_SIZE * 0.75)
        decoded_out = run_one_trial(trial_length, num_connections)
        metrics_dict = parse_wrk_decoded_stdout(decoded_out)
        final_result.append(metrics_dict)

    print(f"\nFinal results: {final_result}\n")

    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/single_deployment_1k_noop_replica.json")
    with open(test_output_json, "wt") as f:
        json.dump(final_result, f)


if __name__ == "__main__":
    main()
