import grpc
import time
from ray import serve
from ray.serve.drivers import DefaultgRPCDriver, DAGDriver
import asyncio
import aiohttp
from ray.serve.generated import serve_pb2, serve_pb2_grpc
import numpy as np
import click
import starlette
from typing import Optional
from serve_test_utils import save_test_results


@serve.deployment(
    num_replicas=1,
)
class D:
    def __call__(self, input):
        return input["a"]


async def measure_grpc_throughput_tps(data_size=1, duration_secs=10):

    async with grpc.aio.insecure_channel("localhost:9000") as channel:
        stub = serve_pb2_grpc.PredictAPIsServiceStub(channel)
        tps_stats = []
        data = bytes("123" * data_size, "utf-8")
        for _ in range(duration_secs):
            start = time.time()
            request_completed = 0
            while time.time() - start < 1:
                _ = await stub.Predict(serve_pb2.PredictRequest(input={"a": data}))
                request_completed += 1
            tps_stats.append(request_completed)

    return tps_stats


async def measure_http_throughput_tps(data_size=1, duration_secs=10):

    tps_stats = []
    data = "123" * data_size

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


async def trial(measure_func, data_size=1, num_clients=1):

    client_tasks = [measure_func for _ in range(num_clients)]

    throughput_stats_tps_list = await asyncio.gather(
        *[client_task(data_size) for client_task in client_tasks]
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
@click.option("--http-test", is_flag=True, type=bool, default=False)
@click.option(
    "--data-size",
    default="1",
    type=int,
)
def main(http_test: Optional[bool], data_size: Optional[int]):

    test_name = "gRPC"
    if http_test:
        test_name = "http"
        serve.run(DAGDriver.bind(D.bind(), http_adapter=json_resolver))
        throughput_mean_tps, throughput_std_tps = asyncio.run(
            trial(measure_http_throughput_tps, data_size=data_size)
        )
    else:
        serve.run(DefaultgRPCDriver.bind(D.bind()))
        throughput_mean_tps, throughput_std_tps = asyncio.run(
            trial(measure_grpc_throughput_tps, data_size=data_size)
        )

    print(throughput_mean_tps, throughput_std_tps)

    save_test_results(
        {test_name: throughput_mean_tps},
        default_output_file=f"/tmp/serve_protocol_{test_name}_benchmark.json",
    )


if __name__ == "__main__":
    main()
