import grpc
import time
from ray import serve
from ray.serve.drivers import gRPCDriver, DAGDriver
import asyncio
import aiohttp
from ray.serve.generated import serve_pb2, serve_pb2_grpc
import numpy as np
import click
import starlette
from typing import Optional


@serve.deployment(
    num_replicas=1,
)
class D:
    def __call__(self, input):
        return input["a"]


async def measure_grpc_throughput_tps(duration_secs=10):

    async with grpc.aio.insecure_channel("localhost:9000") as channel:
        stub = serve_pb2_grpc.PredictAPIsServiceStub(channel)
        tps_stats = []
        data = bytes("123", "utf-8")
        for _ in range(duration_secs):
            start = time.time()
            request_completed = 0
            while time.time() - start < 1:
                _ = await stub.Predict(serve_pb2.PredictRequest(input={"a": data}))
                request_completed += 1
            tps_stats.append(request_completed)

    return tps_stats


async def measure_http_throughput_tps(duration_secs=10):

    tps_stats = []
    data = "123"

    async def fetch(session):
        async with session.get("http://localhost:8000/", json={"a": data}) as response:
            return await response.text()

    async with aiohttp.ClientSession() as session:
        for _ in range(duration_secs):
            start = time.time()
            request_completed = 0
            while time.time() - start < 1:
                _ = await fetch(session)
                request_completed += 1
            tps_stats.append(request_completed)

    return tps_stats


async def trial(measure_func, num_clients=1):

    client_tasks = [measure_func for _ in range(num_clients)]

    throughput_stats_tps_list = await asyncio.gather(
        *[client_task() for client_task in client_tasks]
    )
    throughput_stats_tps = []
    for client_rst in throughput_stats_tps_list:
        throughput_stats_tps.extend(client_rst)

    mean = round(np.mean(throughput_stats_tps), 2)
    std = round(np.std(throughput_stats_tps), 2)
    return mean, std


async def json_resolver(request: starlette.requests.Request):
    return await request.json()


@click.command()
@click.option("--http-test", type=bool, default=False)
def main(http_test: Optional[bool]):

    loop = asyncio.get_event_loop()
    if http_test:
        serve.run(DAGDriver.bind(D.bind(), http_adapter=json_resolver))
        throughput_mean_tps, throughput_std_tps = loop.run_until_complete(
            trial(measure_http_throughput_tps)
        )
    else:
        serve.run(gRPCDriver.bind(D.bind()))
        throughput_mean_tps, throughput_std_tps = loop.run_until_complete(
            trial(measure_grpc_throughput_tps)
        )

    print(throughput_mean_tps, throughput_std_tps)


if __name__ == "__main__":
    main()
