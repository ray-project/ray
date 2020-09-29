# A test that stresses the serve handles. We spin up a backend with a bunch
# of replicas that just forward requests to another backend.

import ray
from ray import serve
from ray.serve import BackendConfig
import logging

num_replicas = 3
num_queries = 10

ray.init(address="auto")
client = serve.start()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def hello_world(_):
    return "Hello World"


class ForwardActor:
    def __init__(self):
        self.handle = client.get_handle("hello_world")

    def __call__(self, _):
        return ray.get(self.handle.remote())


# Here the handle is created each time the function is called, so it may stress
# the system more.
def forward(_):
    handle = client.get_handle("hello_world")
    return ray.get(handle.remote())


config = BackendConfig(num_replicas=num_replicas)

client.create_backend("forward", forward, config=config)
client.create_endpoint("forward", backend="forward")

client.create_backend("hello_world", hello_world)
client.create_endpoint("hello_world", backend="hello_world")

client.create_backend("forwardActor", forwardActor, config=config)
client.create_endpoint("forwardActor", backend="forwardActor")

handle = client.get_handle("forwardActor")
logger.info("Starting serve handle stress testing (actor)")
for _ in range(num_queries):
    ray.get(handle.remote())
logger.info("Finished serve handle stress testing (actor)")

handle = client.get_handle("forward")
logger.info("Starting serve handle stress testing (function)")
for _ in range(num_queries):
    ray.get(handle.remote())
logger.info("Finished serve handle stress testing (function)")
