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
num_nodes = 5
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
        dashboard_host="0.0.0.0")

print("Downloading load testing tool")
subprocess.call([
    "bash", "-c", "rm hey_linux_amd64 || true;"
    "wget https://storage.googleapis.com/hey-release/hey_linux_amd64;"
    "chmod +x hey_linux_amd64"
])

ray.init(address=cluster.address, dashboard_host="0.0.0.0")
serve.init()


@serve.accept_batch
def echo(_):
    time.sleep(0.01)  # Sleep for 10ms
    ray.show_in_webui(str(serve.context.batch_size), key="Current batch size")
    return ["hi {}".format(i) for i in range(serve.context.batch_size)]


config = {"num_replicas": 30, "max_batch_size": 16}
serve.create_backend("echo:v1", echo, config=config)
serve.create_endpoint("echo", backend="echo:v1", route="/echo")

print("Warming up")
for _ in range(5):
    resp = requests.get("http://127.0.0.1:8000/echo").text
    print(resp)
    time.sleep(0.5)

connections = int(config["num_replicas"] * config["max_batch_size"] * 0.75)

while True:
    proc = subprocess.Popen(
        [
            "./hey_linux_amd64", "-c",
            str(connections), "-z", "60m", "http://127.0.0.1:8000/echo"
        ],
        stdout=PIPE,
        stderr=PIPE)
    print("started load testing")
    proc.wait()
    out, err = proc.communicate()
    print(out.decode())
    print(err.decode())
