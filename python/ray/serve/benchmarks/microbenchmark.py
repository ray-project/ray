import aiohttp
import asyncio
import time

import ray
from ray import serve

import numpy as np

NUM_CLIENTS = 8
CALLS_PER_BATCH = 100


@serve.accept_batch
def backend(args):
    return [b"ok" for arg in args]


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
    print("\t{} {} +- {} requests/s".format(name, round(np.mean(stats), 2),
                                            round(np.std(stats), 2)))


async def fetch(session, data):
    async with session.get("http://127.0.0.1:8000/api", data=data) as response:
        await response.text()


@ray.remote
class Client:
    def __init__(self):
        self.session = aiohttp.ClientSession()

    async def do_queries(self, num, data):
        for _ in range(num):
            await fetch(self.session, data)


async def trial(actors, session, data_size):
    if data_size == "small":
        data = None
    elif data_size == "large":
        data = b"a" * 1024 * 1024
    else:
        raise ValueError("data_size should be 'small' or 'large'.")

    async def single_client():
        for _ in range(CALLS_PER_BATCH):
            await fetch(session, data)

    await timeit(
        "single client {} data".format(data_size),
        single_client,
        multiplier=CALLS_PER_BATCH)

    async def many_clients():
        ray.get([a.do_queries.remote(CALLS_PER_BATCH, data) for a in actors])

    await timeit(
        "{} clients {} data".format(len(actors), data_size),
        many_clients,
        multiplier=CALLS_PER_BATCH * len(actors))


async def main():
    ray.init(log_to_driver=False)
    serve.init()

    serve.create_backend("backend", backend)
    serve.create_endpoint("endpoint", backend="backend", route="/api")

    actors = [Client.remote() for _ in range(NUM_CLIENTS)]
    for num_replicas in [1, 8]:
        for backend_config in [
            {
                "max_batch_size": 1,
                "max_concurrent_queries": 1
            },
            {
                "max_batch_size": 1,
                "max_concurrent_queries": 10000
            },
            {
                "max_batch_size": 10000,
                "max_concurrent_queries": 10000
            },
        ]:
            backend_config["num_replicas"] = num_replicas
            serve.update_backend_config("backend", backend_config)
            print(repr(backend_config) + ":")
            async with aiohttp.ClientSession() as session:
                # TODO(edoakes): large data causes broken pipe errors.
                for data_size in ["small"]:
                    await trial(actors, session, data_size)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
