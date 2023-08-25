# Runs some request ping to compare HTTP and gRPC performances in TPS and latency.

import aiohttp
import asyncio
from grpc import aio
import logging
from pprint import pprint
import time
from typing import Callable, Dict, Union

import numpy as np
from starlette.requests import Request

import ray
from ray import serve
from ray.serve.handle import RayServeHandle
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.config import gRPCOptions
from random import random

NUM_CLIENTS = 8
CALLS_PER_BATCH = 100
DELTA = 10**-7


async def timeit(name: str, fn: Callable, multiplier: int = CALLS_PER_BATCH):
    # warmup
    start = time.time()
    while time.time() - start < 0.1:
        await fn()
    # real run
    stats = []
    for _ in range(1):
        count = 0
        start = time.time()
        while time.time() - start < 10:
            await fn()
            count += 1
        end = time.time()
        stats.append(multiplier * count / (end - start))
    tps_mean = round(np.mean(stats), 2)
    tps_std = round(np.std(stats), 2)
    print(f"\t{name} {tps_mean} +- {tps_std} requests/s")

    return tps_mean


async def fetch(session, data):
    async with session.get("http://localhost:8000/", data=data) as response:
        response = await response.text()
        assert response == "ok", response


async def fetch_grpc(stub, data):
    response = await stub.grpc_call(serve_pb2.RawData(nums=data))
    # print("Model output: ", response.output)


@ray.remote
class Client:
    def __init__(self):
        self.session = aiohttp.ClientSession()

    def ready(self):
        return "ok"

    async def do_queries(self, num, data):
        for _ in range(num):
            await fetch(self.session, data)


@ray.remote
class gRPCClient:
    def __init__(self):
        channel = aio.insecure_channel("localhost:9000")
        self.stub = serve_pb2_grpc.RayServeBenchmarkServiceStub(channel)

    def ready(self):
        return "ok"

    async def do_queries(self, num, data):
        for _ in range(num):
            await fetch_grpc(self.stub, data)

    async def time_queries(self, num, data):
        stats = []
        for _ in range(num):
            start = time.time()
            await fetch_grpc(self.stub, data)
            end = time.time()
            stats.append(end - start)
        return stats


def build_app(
    num_replicas: int,
    max_concurrent_queries: int,
    data_size: int,
):
    @serve.deployment(max_concurrent_queries=1000)
    class DataPreprocessing:
        def __init__(self, handle: RayServeHandle):
            self._handle = handle

            # Turn off access log.
            logging.getLogger("ray.serve").setLevel(logging.WARNING)

        async def __call__(self, req: Request):
            # TODO: fixit
            obj_ref = await self._handle.remote(await req.body())
            return await obj_ref

        async def grpc_call(self, raq_data):
            raw = np.asarray(raq_data.nums)
            processed = (raw - np.min(raw)) / (np.max(raw) - np.min(raw) + DELTA)
            model_output_ref = await self._handle.remote(processed)
            return serve_pb2.ModelOutput(output=await model_output_ref)

    @serve.deployment(
        num_replicas=num_replicas,
        max_concurrent_queries=max_concurrent_queries,
    )
    class ModelInference:
        def __init__(self):
            # Turn off access log.
            logging.getLogger("ray.serve").setLevel(logging.WARNING)
            self._model = np.random.randn(data_size, data_size)

        async def __call__(self, processed: np.ndarray) -> float:
            model_output = np.dot(processed, self._model)
            return sum(model_output)

    return DataPreprocessing.bind(ModelInference.bind())


async def trial(
    num_replicas: int,
    max_concurrent_queries: int,
    data_size: int,
    num_clients: int,
) -> Dict[str, float]:
    results = {}

    trial_key_base = (
        f"replica:{num_replicas}/"
        f"concurrent_queries:{max_concurrent_queries}/"
        f"data_size:{data_size}"
    )

    print(
        f"num_replicas={num_replicas},"
        f"max_concurrent_queries={max_concurrent_queries},"
        f"data_size={data_size}"
    )

    app = build_app(
        num_replicas=num_replicas,
        max_concurrent_queries=max_concurrent_queries,
        data_size=data_size,
    )
    serve.run(app)

    data = [random() for _ in range(data_size)]

    # async with aio.insecure_channel("localhost:9000") as channel:
    #     stub = serve_pb2_grpc.RayServeBenchmarkServiceStub(channel)
    #
    #     async def single_client():
    #         for _ in range(CALLS_PER_BATCH):
    #             await fetch_grpc(stub, data)
    #
    #     single_client_avg_tps = await timeit(
    #         name=f"single client {data_size} data size",
    #         fn=single_client,
    #     )
    #     key = f"num_client:1/{trial_key_base}"
    #     results[key] = single_client_avg_tps

    clients = [gRPCClient.remote() for _ in range(num_clients)]
    ray.get([client.ready.remote() for client in clients])

    async def get_query_latencies():
        return ray.get([a.time_queries.remote(CALLS_PER_BATCH, data) for a in clients])

    async def many_clients():
        ray.get([a.do_queries.remote(CALLS_PER_BATCH, data) for a in clients])

    many_client_results = np.asarray(await get_query_latencies())
    many_client_results.flatten()
    print("many_client_results", many_client_results)
    latency_mean = np.mean(many_client_results)
    print("many_client_results", latency_mean)
    avg_tps = await timeit(
        f"{len(clients)} clients {data_size} data size",
        many_clients,
    )
    #
    # results[f"num_client:{len(clients)}/{trial_key_base}"] = multi_client_avg_tps

    return results


async def main():
    results = {}

    results.update(
        await trial(
            num_replicas=1,
            max_concurrent_queries=1,
            data_size=1,
            num_clients=1,
        )
    )
    results.update(
        await trial(
            num_replicas=1,
            max_concurrent_queries=1,
            data_size=2,
            num_clients=1,
        )
    )
    # for num_replicas in [1, 8]:
    #     for max_batch_size, max_concurrent_queries in [
    #         (1, 1),
    #         (1, 10000),
    #         (10000, 10000),
    #     ]:
    #         for data_size in ["small"]:
    #             results.update(
    #                 await trial(
    #                     num_replicas,
    #                     max_batch_size,
    #                     max_concurrent_queries,
    #                     data_size,
    #                 )
    #             )

    print("Results from all conditions:")
    pprint(results)
    return results


if __name__ == "__main__":
    ray.init()

    grpc_port = 9000
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_RayServeBenchmarkServiceServicer_to_server",
    ]
    serve.start(
        grpc_options=gRPCOptions(
            port=grpc_port,
            grpc_servicer_functions=grpc_servicer_functions,
        )
    )
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
