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


async def timeit(name: str, fn: Callable, multiplier: int = 1):
    # warmup
    start = time.time()
    while time.time() - start < 0.1:
        await fn()
    # real run
    stats = []
    for _ in range(1):
        start = time.time()
        count = 0
        while time.time() - start < 0.1:
            await fn()
            count += 1
        end = time.time()
        stats.append(multiplier * count / (end - start))
    print(
        "\t{} {} +- {} requests/s".format(
            name, round(np.mean(stats), 2), round(np.std(stats), 2)
        )
    )
    return round(np.mean(stats), 2)


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
            return obj_ref

        async def grpc_call(self, raq_data):
            raw = np.asarray(raq_data.nums)
            processed = (raw - np.min(raw)) / (np.max(raw) - np.min(raw))
            model_output = await self._handle.remote(processed)
            return serve_pb2.ModelOutput(output=model_output)

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

    async with aio.insecure_channel("localhost:9000") as channel:
        stub = serve_pb2_grpc.RayServeBenchmarkServiceStub(channel)
        response = await stub.grpc_call(serve_pb2.RawData(nums=data))
        print("Model output: " + response.output)

    # async with aiohttp.ClientSession() as session:
    #     async def single_client():
    #         for _ in range(CALLS_PER_BATCH):
    #             await fetch(session, data)
    #
    #     single_client_avg_tps = await timeit(
    #         "single client {} data".format(data_size),
    #         single_client,
    #         multiplier=CALLS_PER_BATCH,
    #     )
    #     key = f"num_client:1/{trial_key_base}"
    #     results[key] = single_client_avg_tps
    #
    # clients = [Client.remote() for _ in range(NUM_CLIENTS)]
    # ray.get([client.ready.remote() for client in clients])
    #
    # async def many_clients():
    #     ray.get([a.do_queries.remote(CALLS_PER_BATCH, data) for a in clients])
    #
    # multi_client_avg_tps = await timeit(
    #     "{} clients {} data".format(len(clients), data_size),
    #     many_clients,
    #     multiplier=CALLS_PER_BATCH * len(clients),
    # )
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
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
        "ray.serve.generated.serve_pb2_grpc.add_FruitServiceServicer_to_server",
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
