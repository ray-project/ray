# Runs several scenarios with varying max batch size, max concurrent queries,
# number of replicas, and with intermediate serve handles (to simulate ensemble
# models) either on or off.

import aiohttp
import asyncio
import time
import requests

import numpy as np

import ray
from ray import serve
from ray.serve.utils import logger

NUM_CLIENTS = 8
CALLS_PER_BATCH = 100


async def timeit(name, fn, multiplier=1):
    # warmup
    start = time.time()
    while time.time() - start < 1:
        await fn()
    # real run
    stats = []
    for _ in range(4):
        start = time.time()
        count = 0
        while time.time() - start < 2:
            await fn()
            count += 1
        end = time.time()
        stats.append(multiplier * count / (end - start))
    logger.info(
        "\t{} {} +- {} requests/s".format(
            name, round(np.mean(stats), 2), round(np.std(stats), 2)
        )
    )
    return round(np.mean(stats), 2)


async def fetch(session, data):
    async with session.get("http://localhost:8000/api", data=data) as response:
        response = await response.text()
        assert response == "ok", response


@ray.remote
class Client:
    def __init__(self):
        self.session = aiohttp.ClientSession()

    def ready(self):
        return "ok"

    async def do_queries(self, num, data):
        for _ in range(num):
            await fetch(self.session, data)


async def trial(
    result_json,
    intermediate_handles,
    num_replicas,
    max_batch_size,
    max_concurrent_queries,
    data_size,
):
    trial_key_base = (
        f"replica:{num_replicas}/batch_size:{max_batch_size}/"
        f"concurrent_queries:{max_concurrent_queries}/"
        f"data_size:{data_size}/intermediate_handle:{intermediate_handles}"
    )

    logger.info(
        f"intermediate_handles={intermediate_handles},"
        f"num_replicas={num_replicas},"
        f"max_batch_size={max_batch_size},"
        f"max_concurrent_queries={max_concurrent_queries},"
        f"data_size={data_size}"
    )

    deployment_name = "api"
    if intermediate_handles:
        deployment_name = "downstream"

        @serve.deployment(name="api", max_concurrent_queries=1000)
        class ForwardActor:
            def __init__(self):
                self.handle = None

            async def __call__(self, req):
                if self.handle is None:
                    self.handle = serve.get_deployment(deployment_name).get_handle(
                        sync=False
                    )
                obj_ref = await self.handle.remote(req)
                return await obj_ref

        ForwardActor.deploy()
        routes = requests.get("http://localhost:8000/-/routes").json()
        assert "/api" in routes, routes

    @serve.deployment(
        name=deployment_name,
        num_replicas=num_replicas,
        max_concurrent_queries=max_concurrent_queries,
    )
    class D:
        @serve.batch(max_batch_size=max_batch_size)
        async def batch(self, reqs):
            return [b"ok"] * len(reqs)

        async def __call__(self, req):
            if max_batch_size > 1:
                return await self.batch(req)
            else:
                return b"ok"

    D.deploy()
    routes = requests.get("http://localhost:8000/-/routes").json()
    assert f"/{deployment_name}" in routes, routes

    if data_size == "small":
        data = None
    elif data_size == "large":
        data = b"a" * 1024 * 1024
    else:
        raise ValueError("data_size should be 'small' or 'large'.")

    async with aiohttp.ClientSession() as session:

        async def single_client():
            for _ in range(CALLS_PER_BATCH):
                await fetch(session, data)

        single_client_avg_tps = await timeit(
            "single client {} data".format(data_size),
            single_client,
            multiplier=CALLS_PER_BATCH,
        )
        key = "num_client:1/" + trial_key_base
        result_json.update({key: single_client_avg_tps})

    clients = [Client.remote() for _ in range(NUM_CLIENTS)]
    ray.get([client.ready.remote() for client in clients])

    async def many_clients():
        ray.get([a.do_queries.remote(CALLS_PER_BATCH, data) for a in clients])

    multi_client_avg_tps = await timeit(
        "{} clients {} data".format(len(clients), data_size),
        many_clients,
        multiplier=CALLS_PER_BATCH * len(clients),
    )
    key = f"num_client:{len(clients)}/" + trial_key_base
    result_json.update({key: multi_client_avg_tps})

    logger.info(result_json)


async def main():
    result_json = {}
    for intermediate_handles in [False, True]:
        for num_replicas in [1, 8]:
            for max_batch_size, max_concurrent_queries in [
                (1, 1),
                (1, 10000),
                (10000, 10000),
            ]:
                # TODO(edoakes): large data causes broken pipe errors.
                for data_size in ["small"]:
                    await trial(
                        result_json,
                        intermediate_handles,
                        num_replicas,
                        max_batch_size,
                        max_concurrent_queries,
                        data_size,
                    )
    return result_json


if __name__ == "__main__":
    ray.init()
    serve.start()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
