# Runs some request ping to compare HTTP and gRPC performances in TPS and latency.

import aiohttp
import asyncio
from grpc import aio, StatusCode
import logging
from pprint import pprint
import time
from typing import Callable, Dict

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


async def get_query_tps(name: str, fn: Callable, multiplier: int = CALLS_PER_BATCH):
    """Get query TPS.

    Run the function for 0.5 seconds 10 times to calculate how many requests can
    be completed. And use those stats to calculate the mean and std of TPS.
    """
    # warmup
    start = time.time()
    while time.time() - start < 0.1:
        await fn()
    # real run
    stats = []
    for _ in range(10):
        count = 0
        start = time.time()
        while time.time() - start < 0.5:
            await fn()
            count += 1
        end = time.time()
        stats.append(multiplier * count / (end - start))
    tps_mean = round(np.mean(stats), 2)
    tps_std = round(np.std(stats), 2)
    print(f"\t{name} {tps_mean} +- {tps_std} requests/s")

    return tps_mean, tps_std


async def get_query_latencies(name: str, fn: Callable):
    """Get query latencies.

    Take all the latencies from the function and calculate the mean and std.
    """
    many_client_results = np.asarray(await fn())
    many_client_results.flatten()
    latency_ms_mean = round(np.mean(many_client_results) * 1000, 2)
    latency_ms_std = round(np.std(many_client_results) * 1000, 2)
    print(f"\t{name} {latency_ms_mean} +- {latency_ms_std} ms")

    return latency_ms_mean, latency_ms_std


async def fetch_http(session, data):
    # async with session.get("http://localhost:8000/", data=data) as response:
    #     response = await response.text()
    #     assert response == "ok", response

    response = await session.get("http://localhost:8000/", data=data)
    print("response", response.text())
    assert response.status == 200, response


async def fetch_grpc(stub, data):
    await stub.grpc_call(serve_pb2.RawData(nums=data))


@ray.remote
class HTTPClient:
    def __init__(self):
        self.session = aiohttp.ClientSession()

    def ready(self):
        return "ok"

    async def do_queries(self, num, data):
        for _ in range(num):
            await fetch_http(self.session, data)

    async def time_queries(self, num, data):
        stats = []
        for _ in range(num):
            start = time.time()
            await fetch_http(self.session, data)
            end = time.time()
            stats.append(end - start)
        return stats


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

        def normalize(self, raw: np.ndarray) -> np.ndarray:
            return (raw - np.min(raw)) / (np.max(raw) - np.min(raw) + DELTA)

        async def __call__(self, req: Request):
            raw = np.asarray(await req.body())
            processed = self.normalize(raw)
            model_output_ref = await self._handle.remote(processed)
            return await model_output_ref

        async def grpc_call(self, raq_data):
            raw = np.asarray(raq_data.nums)
            processed = self.normalize(raw)
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
    test_grpc: bool = False,
) -> Dict[str, float]:
    results = {}

    trial_key_base = (
        f"replica:{num_replicas}/"
        f"concurrent_queries:{max_concurrent_queries}/"
        f"data_size:{data_size}"
    )

    app = build_app(
        num_replicas=num_replicas,
        max_concurrent_queries=max_concurrent_queries,
        data_size=data_size,
    )
    serve.run(app)

    data = [random() for _ in range(data_size)]

    # clients = [gRPCClient.remote() for _ in range(num_clients)]
    clients = [HTTPClient.remote() for _ in range(num_clients)]
    ray.get([client.ready.remote() for client in clients])

    async def client_time_queries():
        return ray.get([a.time_queries.remote(CALLS_PER_BATCH, data) for a in clients])

    async def client_do_queries():
        ray.get([a.do_queries.remote(CALLS_PER_BATCH, data) for a in clients])

    metrics_name = f"{len(clients)} clients {data_size} data size"
    tps_mean, tps_sdt = await get_query_tps(
        metrics_name,
        client_do_queries,
    )
    latency_ms_mean, latency_ms_std = await get_query_latencies(
        metrics_name,
        client_time_queries,
    )

    results[f"num_client:{len(clients)}/{trial_key_base}"] = {
        "tps_mean": tps_mean,
        "tps_sdt": tps_sdt,
        "latency_ms_mean": latency_ms_mean,
        "latency_ms_std": latency_ms_std,
    }

    return results


async def main():
    results = {}

    for num_replicas in [1, 8]:
        for max_concurrent_queries in [1, 10000]:
            for data_size in [1, 100, 10_000]:
                for num_clients in [1, 8]:
                    results.update(
                        await trial(
                            num_replicas=num_replicas,
                            max_concurrent_queries=max_concurrent_queries,
                            data_size=data_size,
                            num_clients=num_clients,
                        )
                    )

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
