# A test that stresses the serve handles. We spin up a backend with a bunch
# (0-2) of replicas that just forward requests to another backend.
#
# By comparing using the forward replicas with just calling the worker
# replicas, we measure the (latency) overhead in the handle. This answers
# the question: How much of a latency/throughput hit is there when I
# compose models?
#
# By comparing the qps as we fix the number of forwarder replicas and vary the
# number of worker replicas, we measure the limit of a single async actor. This
# answers the question: How many "ForwardActor"s or these kinds of high-level
# pipeline workers do I need to provision for my workload? every 1k qps,
# 2k qps?
#
# Sample output:
# INFO:__main__:0 forwarders and 1 worker replicas: 534 requests/s
# INFO:__main__:0 forwarders and 5 worker replicas: 508 requests/s
# INFO:__main__:0 forwarders and 10 worker replicas: 466 requests/s
# INFO:__main__:1 forwarders and 1 worker replicas: 273 requests/s
# INFO:__main__:1 forwarders and 5 worker replicas: 258 requests/s
# INFO:__main__:1 forwarders and 10 worker replicas: 254 requests/s
# INFO:__main__:2 forwarders and 1 worker replicas: 268 requests/s
# INFO:__main__:2 forwarders and 5 worker replicas: 250 requests/s
# INFO:__main__:2 forwarders and 10 worker replicas: 250 requests/s

import ray
from ray import serve
from ray.serve import BackendConfig
import logging
import time

ray.init(address="auto")

client = serve.start()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def hello_world(_):
    return b"Hello World"


class ForwardActor:
    def __init__(self):
        client = serve.connect()
        self.handle = client.get_handle("hello_world")

    async def __call__(self, _):
        return ray.get(self.handle.remote())


client.create_backend("hello_world", hello_world)
client.create_endpoint("hello_world", backend="hello_world")

client.create_backend("ForwardActor", ForwardActor)
client.create_endpoint("ForwardActor", backend="ForwardActor")


def run_test(num_replicas, num_forwarders):
    replicas_config = BackendConfig(num_replicas=num_replicas)
    client.update_backend_config("hello_world", replicas_config)

    if (num_forwarders == 0):
        handle = client.get_handle("hello_world")
    else:
        forwarders_config = BackendConfig(num_replicas=num_forwarders)
        client.update_backend_config("ForwardActor", forwarders_config)
        handle = client.get_handle("ForwardActor")

    # warmup - helpful to wait for gc.collect() and actors to start
    start = time.time()
    while time.time() - start < 1:
        ray.get(handle.remote())

    # real test
    start = time.time()
    count = 0
    while time.time() - start < 1:
        ray.get(handle.remote())
        count += 1
    logger.info("{} forwarders and {} worker replicas: {} requests/s".format(
        num_forwarders, num_replicas, count))


for num_forwarders in [0, 1, 2]:
    for num_replicas in [1, 5, 10]:
        run_test(num_replicas, num_forwarders)
