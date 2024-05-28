# Runs several scenarios with varying max batch size, max concurrent queries,
# number of replicas, and with intermediate serve handles (to simulate ensemble
# models) either on or off.

import asyncio
import logging
from pprint import pprint
from typing import Dict, Union

import aiohttp
from starlette.requests import Request

import ray
from ray import serve
from ray.serve._private.benchmarks.common import run_throughput_benchmark
from ray.serve.handle import DeploymentHandle

NUM_CLIENTS = 8
CALLS_PER_BATCH = 100


async def fetch(session, data):
    async with session.get("http://localhost:8000/", data=data) as response:
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


def build_app(
    intermediate_handles: bool,
    num_replicas: int,
    max_batch_size: int,
    max_ongoing_requests: int,
):
    @serve.deployment(max_ongoing_requests=1000)
    class Upstream:
        def __init__(self, handle: DeploymentHandle):
            self._handle = handle

            # Turn off access log.
            logging.getLogger("ray.serve").setLevel(logging.WARNING)

        async def __call__(self, req: Request):
            return await self._handle.remote(await req.body())

    @serve.deployment(
        num_replicas=num_replicas,
        max_ongoing_requests=max_ongoing_requests,
    )
    class Downstream:
        def __init__(self):
            # Turn off access log.
            logging.getLogger("ray.serve").setLevel(logging.WARNING)

        @serve.batch(max_batch_size=max_batch_size)
        async def batch(self, reqs):
            return [b"ok"] * len(reqs)

        async def __call__(self, req: Union[bytes, Request]):
            if max_batch_size > 1:
                return await self.batch(req)
            else:
                return b"ok"

    if intermediate_handles:
        return Upstream.bind(Downstream.bind())
    else:
        return Downstream.bind()


async def trial(
    intermediate_handles: bool,
    num_replicas: int,
    max_batch_size: int,
    max_ongoing_requests: int,
    data_size: str,
) -> Dict[str, float]:
    results = {}

    trial_key_base = (
        f"replica:{num_replicas}/batch_size:{max_batch_size}/"
        f"concurrent_queries:{max_ongoing_requests}/"
        f"data_size:{data_size}/intermediate_handle:{intermediate_handles}"
    )

    print(
        f"intermediate_handles={intermediate_handles},"
        f"num_replicas={num_replicas},"
        f"max_batch_size={max_batch_size},"
        f"max_ongoing_requests={max_ongoing_requests},"
        f"data_size={data_size}"
    )

    app = build_app(
        intermediate_handles, num_replicas, max_batch_size, max_ongoing_requests
    )
    serve.run(app)

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

        single_client_avg_tps, single_client_std_tps = await run_throughput_benchmark(
            single_client,
            multiplier=CALLS_PER_BATCH,
        )
        print(
            "\t{} {} +- {} requests/s".format(
                "single client {} data".format(data_size),
                single_client_avg_tps,
                single_client_std_tps,
            )
        )
        key = f"num_client:1/{trial_key_base}"
        results[key] = single_client_avg_tps

    clients = [Client.remote() for _ in range(NUM_CLIENTS)]
    ray.get([client.ready.remote() for client in clients])

    async def many_clients():
        ray.get([a.do_queries.remote(CALLS_PER_BATCH, data) for a in clients])

    multi_client_avg_tps, _ = await run_throughput_benchmark(
        many_clients,
        multiplier=CALLS_PER_BATCH * len(clients),
    )

    results[f"num_client:{len(clients)}/{trial_key_base}"] = multi_client_avg_tps
    return results


async def main():
    results = {}
    for intermediate_handles in [False, True]:
        for num_replicas in [1, 8]:
            for max_batch_size, max_ongoing_requests in [
                (1, 1),
                (1, 10000),
                (10000, 10000),
            ]:
                # TODO(edoakes): large data causes broken pipe errors.
                for data_size in ["small"]:
                    results.update(
                        await trial(
                            intermediate_handles,
                            num_replicas,
                            max_batch_size,
                            max_ongoing_requests,
                            data_size,
                        )
                    )

    print("Results from all conditions:")
    pprint(results)
    return results


if __name__ == "__main__":
    ray.init()
    serve.start()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
