import json
import os
import time
import subprocess
from subprocess import PIPE

import requests

import ray
from ray import serve
from ray.cluster_utils import Cluster

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
TIME_PER_CYCLE = "60m"

def update_progress(result):
    """
    Write test result json to /tmp/, which will be read from 
    anyscale product runs in each releaser test
    """
    result["last_update"] = time.time()
    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/release_test_output.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

cluster = Cluster()
for i in range(NUM_NODES):
    cluster.add_node(
        redis_port=6379 if i == 0 else None,
        num_redis_shards=NUM_REDIS_SHARDS if i == 0 else None,
        num_cpus=8,
        num_gpus=0,
        resources={str(i): 2},
        object_store_memory=OBJECT_STORE_MEMORY,
        redis_max_memory=REDIS_MAX_MEMORY,
        dashboard_host="0.0.0.0",
    )

ray.init(address=cluster.address, dashboard_host="0.0.0.0")
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
    print(f"Started load testing with the following config: ")
    print(f"num_connections: {NUM_CONNECTIONS}")
    print(f"num_threads: {NUM_THREADS}")
    print(f"time_per_cycle: {TIME_PER_CYCLE}")

    proc.wait()
    out, err = proc.communicate()
    print(out.decode())
    print(err.decode())

    update_progress({
        "stdout": out.decode(),
        "stderr": err.decode(),
    })
