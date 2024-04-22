import click
import re
import time
import subprocess
from subprocess import PIPE

import requests
import numpy as np

import ray
from ray import serve
from ray.cluster_utils import Cluster
from ray._private.test_utils import safe_write_to_results_json
from ray.serve._private.test_utils import convert_metrics_to_perf_metrics

# Global variables / constants appear only right after imports.
# Ray serve deployment setup constants
DEFAULT_NUM_REPLICAS = 7
DEFAULT_MAX_BATCH_SIZE = 16

# Cluster setup constants
DEFAULT_NUM_REDIS_SHARDS = 1
DEFAULT_REDIS_MAX_MEMORY = 10**8
DEFAULT_OBJECT_STORE_MEMORY = 10**8
DEFAULT_NUM_NODES = 4

# wrk setup constants (might want to make these configurable ?)
DEFAULT_NUM_CONNECTIONS = int(DEFAULT_NUM_REPLICAS * DEFAULT_MAX_BATCH_SIZE * 0.75)
DEFAULT_NUM_THREADS = 2
# Append and print every 5mins for quick status polling as well
# as time series plotting
TIME_PER_CYCLE = "5m"


def update_progress(result):
    """
    Write test result json to /tmp/, which will be read from
    anyscale product runs in each releaser test
    """
    result["last_update"] = time.time()
    safe_write_to_results_json(result)


@click.command()
@click.option("--num-replicas", type=int, default=DEFAULT_NUM_REPLICAS)
@click.option("--num-connections", type=int, default=DEFAULT_NUM_CONNECTIONS)
@click.option("--num-threads", type=int, default=DEFAULT_NUM_THREADS)
@click.option("--num-nodes", type=int, default=DEFAULT_NUM_NODES)
@click.option("--num-redis-shards", type=int, default=DEFAULT_NUM_REDIS_SHARDS)
@click.option("--redis-max-memory", type=int, default=DEFAULT_REDIS_MAX_MEMORY)
@click.option("--object-store-memory", type=int, default=DEFAULT_OBJECT_STORE_MEMORY)
@click.option("--max-batch-size", type=int, default=DEFAULT_MAX_BATCH_SIZE)
def main(
    num_replicas: int,
    num_connections: int,
    num_threads: int,
    num_nodes: int,
    num_redis_shards: int,
    redis_max_memory: int,
    object_store_memory: int,
    max_batch_size: int,
):
    # Dynamically define Serve deployment
    @serve.deployment(name="echo", num_replicas=num_replicas)
    class Echo:
        @serve.batch(max_batch_size=max_batch_size)
        async def handle_batch(self, requests):
            time.sleep(0.01)
            return ["hi" for _ in range(len(requests))]

        async def __call__(self, request):
            return await self.handle_batch(request)

    cluster = Cluster()
    for i in range(num_nodes):
        cluster.add_node(
            redis_port=6379 if i == 0 else None,
            num_redis_shards=num_redis_shards if i == 0 else None,
            dashboard_agent_listen_port=(52365 + i),
            num_cpus=8,
            num_gpus=0,
            resources={str(i): 2},
            object_store_memory=object_store_memory,
            redis_max_memory=redis_max_memory,
            dashboard_host="0.0.0.0",
        )

    ray.init(address=cluster.address, log_to_driver=False, dashboard_host="0.0.0.0")
    serve.run(Echo.bind(), route_prefix="/echo")

    print("Warming up")
    for _ in range(5):
        resp = requests.get("http://127.0.0.1:8000/echo").text
        print(resp)
        time.sleep(0.5)

    print("Started load testing with the following config: ")
    print(f"num_connections: {num_connections}")
    print(f"num_threads: {num_threads}")
    print(f"time_per_cycle: {TIME_PER_CYCLE}")

    iteration = 0
    latencies = []
    throughputs = []
    while True:
        proc = subprocess.Popen(
            [
                "wrk",
                "-c",
                str(num_connections),
                "-t",
                str(num_threads),
                "-d",
                TIME_PER_CYCLE,
                "http://127.0.0.1:8000/echo",
            ],
            stdout=PIPE,
            stderr=PIPE,
        )
        proc.wait()
        out, err = proc.communicate()
        # Check if wrk command succeeded. If this happens repeatedly, the release test
        # infrastructure will correctly fail the test with "Last update to results json
        # was too long ago."
        if proc.returncode != 0:
            print("wrk failed with the following error: ")
            print(err)
            print("Will try again in 5 seconds")
            time.sleep(5)
            continue

        # Sample wrk stdout:
        #
        # Running 10s test @ http://127.0.0.1:8000/echo
        # 2 threads and 84 connections
        # Thread Stats   Avg      Stdev     Max   +/- Stdev
        #     Latency    59.33ms   13.51ms 113.83ms   64.20%
        #     Req/Sec   709.16     61.73   848.00     78.50%
        # 14133 requests in 10.02s, 2.08MB read
        # Requests/sec:   1410.71
        # Transfer/sec:    212.16KB
        print(f"Iteration {iteration}:")
        print(out.decode())
        print(err.decode())

        # perf_metrics = []
        for line in out.decode().splitlines():
            parsed = re.split(r"\s+", line.strip())
            if parsed[0] == "Latency":
                latencies.append(float(parsed[1].rstrip("ms")))
            elif parsed[0] == "Req/Sec":
                continue
            elif parsed[0] == "Requests/sec:":
                throughputs.append(float(parsed[1]))
            else:
                continue

        perf_metrics = convert_metrics_to_perf_metrics(
            {
                "avg_latency": np.mean(latencies),
                "avg_rps": np.mean(throughputs),
            }
        )
        iteration += 1
        update_progress({"iterations": iteration, "perf_metrics": perf_metrics})


if __name__ == "__main__":
    main()
