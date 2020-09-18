# A test that stresses the serve handles. E.g., spin up a backend with a bunch
# of replicas that just forward requests to another backend.

import ray
from ray import serve
from ray.serve import BackendConfig

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
config = BackendConfig(num_replicas=4)

client.create_backend("forward", forward, config=config)
client.create_endpoint("forward", backend="forward")

client.create_backend("hello_world", hello_world, config=config)
client.create_endpoint("hello_world", backend="hello_world")

handle = client.get_handle("forward")
for _ in range(10):
    print(ray.get(handle.remote()))