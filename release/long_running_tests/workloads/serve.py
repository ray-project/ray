import time
import subprocess
from subprocess import PIPE

import requests

import ray
from ray import serve
from ray.cluster_utils import Cluster

num_redis_shards = 1
redis_max_memory = 10**8
object_store_memory = 10**8
num_nodes = 4
cluster = Cluster()
for i in range(num_nodes):
    cluster.add_node(
        redis_port=6379 if i == 0 else None,
        num_redis_shards=num_redis_shards if i == 0 else None,
        num_cpus=8,
        num_gpus=0,
        resources={str(i): 2},
        object_store_memory=object_store_memory,
        redis_max_memory=redis_max_memory,
        dashboard_host="0.0.0.0",
    )

ray.init(address=cluster.address, dashboard_host="0.0.0.0")
client = serve.start()


@serve.accept_batch
def echo(requests_batch):
    time.sleep(0.01)  # Sleep for 10ms
    return ["hi" for _ in range(len(requests_batch))]


config = {"num_replicas": 7, "max_batch_size": 16}
client.create_backend("echo:v1", echo, config=config)
client.create_endpoint("echo", backend="echo:v1", route="/echo")

print("Warming up")
for _ in range(5):
    resp = requests.get("http://127.0.0.1:8000/echo").text
    print(resp)
    time.sleep(0.5)

connections = int(config["num_replicas"] * config["max_batch_size"] * 0.75)
num_threads = 2
time_to_run = "60m"

while True:
    proc = subprocess.Popen(
        [
            "wrk",
            "-c",
            str(connections),
            "-t",
            str(num_threads),
            "-d",
            time_to_run,
            "http://127.0.0.1:8000/echo",
        ],
        stdout=PIPE,
        stderr=PIPE,
    )
    print("started load testing")
    proc.wait()
    out, err = proc.communicate()
    print(out.decode())
    print(err.decode())
