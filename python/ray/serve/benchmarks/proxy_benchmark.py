# Runs some request ping to compare HTTP and gRPC performances in TPS and latency.

import asyncio
import json
import logging
import time
from pprint import pprint
from random import random
from typing import Callable, Dict

import aiohttp
import numpy as np
import ray
from grpc import aio
from ray import serve
from ray.serve.config import gRPCOptions
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.handle import RayServeHandle
from starlette.requests import Request

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
    data_json = {"nums": data}
    response = await session.get("http://localhost:8000/", json=data_json)
    response_text = await response.read()
    float(response_text.decode())


async def fetch_grpc(stub, data):
    result = await stub.grpc_call(serve_pb2.RawData(nums=data))
    result.output


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
            """HTTP entrypoint.

            It parses the request, normalize the data, and send to model for inference.
            """
            body = json.loads(await req.body())
            raw = np.asarray(body["nums"])
            processed = self.normalize(raw)
            model_output_ref = await self._handle.remote(processed)
            return await model_output_ref

        async def grpc_call(self, raq_data):
            """gRPC entrypoint.

            It parses the request, normalize the data, and send to model for inference.
            """
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
            # Run a dot product with a random matrix to simulate a model inference.
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

    # Generate input data as array of random floats.
    data = [random() for _ in range(data_size)]

    # Build and deploy the app.
    app = build_app(
        num_replicas=num_replicas,
        max_concurrent_queries=max_concurrent_queries,
        data_size=data_size,
    )
    serve.run(app)

    # Start clients.
    if test_grpc:
        clients = [gRPCClient.remote() for _ in range(num_clients)]
    else:
        clients = [HTTPClient.remote() for _ in range(num_clients)]
    ray.get([client.ready.remote() for client in clients])

    async def client_time_queries():
        return ray.get([a.time_queries.remote(CALLS_PER_BATCH, data) for a in clients])

    async def client_do_queries():
        ray.get([a.do_queries.remote(CALLS_PER_BATCH, data) for a in clients])

    trial_key_base = (
        f"proxy:{'gRPC' if test_grpc else 'HTTP'}/"
        f"num_client:{num_clients}/"
        f"replica:{num_replicas}/"
        f"concurrent_queries:{max_concurrent_queries}/"
        f"data_size:{data_size}"
    )
    tps_mean, tps_sdt = await get_query_tps(
        trial_key_base,
        client_do_queries,
    )
    latency_ms_mean, latency_ms_std = await get_query_latencies(
        trial_key_base,
        client_time_queries,
    )

    results[trial_key_base] = {
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
                    for test_grpc in [True, False]:
                        results.update(
                            await trial(
                                num_replicas=num_replicas,
                                max_concurrent_queries=max_concurrent_queries,
                                data_size=data_size,
                                num_clients=num_clients,
                                test_grpc=test_grpc,
                            )
                        )

    print("Results from all conditions:")
    pprint(results)

    for key, stats in results.items():
        key_split = key.split("/")
        proxy = key_split[0]
        num_client = key_split[1].split(":")[1]
        replica = key_split[2].split(":")[1]
        concurrent_queries = key_split[3].split(":")[1]
        data_size = key_split[4].split(":")[1]
        print(
            f"{proxy}\t{num_client}\t{replica}\t{concurrent_queries}\t{data_size}"
            f"\t{stats['latency_ms_mean']}\t{stats['latency_ms_std']}"
            f"\t{stats['tps_mean']}\t{stats['tps_sdt']}"
        )

    return results


if __name__ == "__main__":
    ray.init()

    grpc_port = 9000
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc."
        "add_RayServeBenchmarkServiceServicer_to_server",
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
