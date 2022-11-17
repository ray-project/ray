# A test that stresses the serve handles. We spin up a deployment with a bunch
# (0-2) of replicas that just forward requests to another deployment.
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

import asyncio
import time

import ray
from ray import serve

num_queries = 10000
max_concurrent_queries = 100000

ray.init(address="auto")


@serve.deployment(max_concurrent_queries=max_concurrent_queries)
def worker(*args):
    return b"Hello World"


@serve.deployment(max_concurrent_queries=max_concurrent_queries)
class Forwarder:
    def __init__(self, sync: bool):
        self.sync = sync
        self.handle = worker.get_handle(sync=sync)

    async def __call__(self, *args):
        if self.sync:
            await self.handle.remote()
        else:
            await (await self.handle.remote_async())


async def run_test(num_replicas, num_forwarders, sync):
    serve.start()

    worker.options(num_replicas=num_replicas).deploy()

    if num_forwarders == 0:
        handle = worker.get_handle(sync=sync)
    else:
        Forwarder.options(num_replicas=num_forwarders).deploy(sync)
        handle = Forwarder.get_handle(sync=sync)

    # warmup - helpful to wait for gc.collect() and actors to start
    start = time.time()
    while time.time() - start < 1:
        if sync:
            ray.get(handle.remote())
        else:
            ray.get(await handle.remote_async())

    # real test
    start = time.time()
    if sync:
        ray.get([handle.remote() for _ in range(num_queries)])
    else:
        ray.get([(await handle.remote_async()) for _ in range(num_queries)])
    qps = num_queries / (time.time() - start)

    print(
        f"Sync: {sync}, {num_forwarders} forwarders and {num_replicas} worker "
        f"replicas: {int(qps)} requests/s"
    )
    serve.shutdown()


async def main():
    for sync in [True, False]:
        for num_forwarders in [0, 1, 2]:
            for num_replicas in [1, 5, 10]:
                await run_test(num_replicas, num_forwarders, sync)


asyncio.get_event_loop().run_until_complete(main())
