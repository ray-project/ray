# A test that stresses the serve handles. E.g., spin up a backend with a bunch
# of replicas that just forward requests to another backend.

import ray
from ray import serve
from ray.serve import BackendConfig

num_replicas = 4
num_queries = 10
# TODO(architkulkarni): edit to work on cluster
# ray.init(address="auto")
ray.init()

client = serve.start()

def hello_world(_):
    return "Hello World"

def forward(_):
    handle = client.get_handle("hello_world")
    return ray.get(handle.remote())

# TODO(architkulkarni): edit to work on cluster
config = BackendConfig(num_replicas=num_replicas)

client.create_backend("forward", forward, config=config)
client.create_endpoint("forward", backend="forward")

client.create_backend("hello_world", hello_world)
client.create_endpoint("hello_world", backend="hello_world")

handle = client.get_handle("forward")
print("Starting serve handle stress testing")
for _ in range(num_queries):
    ray.get(handle.remote())
print("Finished serve handle stress testing")