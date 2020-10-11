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
# 0 forwarders and 1 worker replicas: 1282 requests/s
# 0 forwarders and 5 worker replicas: 1375 requests/s
# 0 forwarders and 10 worker replicas: 1362 requests/s
# 1 forwarders and 1 worker replicas: 608 requests/s
# 1 forwarders and 5 worker replicas: 626 requests/s
# 1 forwarders and 10 worker replicas: 627 requests/s
# 2 forwarders and 1 worker replicas: 609 requests/s
# 2 forwarders and 5 worker replicas: 620 requests/s
# 2 forwarders and 10 worker replicas: 609 requests/s

import ray
from ray import serve
from ray.serve import BackendConfig
from ray.serve.utils import logger
import time

num_queries = 2000

ray.init(address="auto")

client = serve.start()


def hello_world(_):
    return b"Hello World"


class ForwardActor:
    def __init__(self):
        client = serve.connect()
        self.handle = client.get_handle("hello_world")

    async def __call__(self, _):
        await self.handle.remote()


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
    ray.get([handle.remote() for _ in range(num_queries)])
    qps = num_queries / (time.time() - start)

    logger.info("{} forwarders and {} worker replicas: {} requests/s".format(
        num_forwarders, num_replicas, int(qps)))


for num_forwarders in [0, 1, 2]:
    for num_replicas in [1, 5, 10]:
        run_test(num_replicas, num_forwarders)
