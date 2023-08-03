import re
import time
import subprocess
from subprocess import PIPE

import requests

import ray
from ray import serve
from ray.cluster_utils import Cluster
from ray._private.test_utils import safe_write_to_results_json

# Global variables / constants appear only right after imports.
# Ray serve deployment setup constants
NUM_REPLICAS = 7
MAX_BATCH_SIZE = 16

# Cluster setup constants
NUM_REDIS_SHARDS = 1
REDIS_MAX_MEMORY = 10**8
OBJECT_STORE_MEMORY = 10**8
NUM_NODES = 4

# wrk setup constants (might want to make these configurable ?)
NUM_CONNECTIONS = int(NUM_REPLICAS * MAX_BATCH_SIZE * 0.75)
NUM_THREADS = 2
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


cluster = Cluster()
for i in range(NUM_NODES):
    cluster.add_node(
        redis_port=6379 if i == 0 else None,
        num_redis_shards=NUM_REDIS_SHARDS if i == 0 else None,
        dashboard_agent_listen_port=(52365 + i),
        num_cpus=8,
        num_gpus=0,
        resources={str(i): 2},
        object_store_memory=OBJECT_STORE_MEMORY,
        redis_max_memory=REDIS_MAX_MEMORY,
        dashboard_host="0.0.0.0",
    )

ray.init(address=cluster.address, log_to_driver=False, dashboard_host="0.0.0.0")
serve.start()


@serve.deployment(name="echo", num_replicas=NUM_REPLICAS)
class Echo:
    @serve.batch(max_batch_size=MAX_BATCH_SIZE)
    async def handle_batch(self, requests):
        time.sleep(0.01)
        return ["hi" for _ in range(len(requests))]

    async def __call__(self, request):
        return await self.handle_batch(request)


Echo.deploy()

print("Warming up")
for _ in range(5):
    resp = requests.get("http://127.0.0.1:8000/echo").text
    print(resp)
    time.sleep(0.5)

print("Started load testing with the following config: ")
print(f"num_connections: {NUM_CONNECTIONS}")
print(f"num_threads: {NUM_THREADS}")
print(f"time_per_cycle: {TIME_PER_CYCLE}")

while True:
    proc = subprocess.Popen(
        [
            "wrk",
            "-c",
            str(NUM_CONNECTIONS),
            "-t",
            str(NUM_THREADS),
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
    metrics_dict = {}
    for line in out.decode().splitlines():
        parsed = re.split(r"\s+", line.strip())
        if parsed[0] == "Latency":
            metrics_dict["latency_avg"] = parsed[1]
            metrics_dict["latency_stdev"] = parsed[2]
            metrics_dict["latency_max"] = parsed[3]
            metrics_dict["latency_+/-_stdev"] = parsed[4]
        elif parsed[0] == "Req/Sec":
            metrics_dict["req/sec_avg"] = parsed[1]
            metrics_dict["req/sec_stdev"] = parsed[2]
            metrics_dict["req/sec_max"] = parsed[3]
            metrics_dict["req/sec_+/-_stdev"] = parsed[4]
        elif parsed[0] == "Requests/sec:":
            metrics_dict["requests/sec"] = parsed[1]
        elif parsed[0] == "Transfer/sec:":
            metrics_dict["transfer/sec"] = parsed[1]

    print(out.decode())
    print(err.decode())

    update_progress(metrics_dict)
