import ray
from ray import serve
from torchvision import models
from typing import List
import torch
from ray.serve.drivers import DAGDriver
from ray.dag.input_node import InputNode
import asyncio
import aiohttp
import starlette
import time
from serve_test_utils import save_test_results
import numpy as np
import click
from typing import Optional
from ray.serve.handle import RayServeHandle


# 8 images as input when batch size increase, we replica the input here
input_uris = [
    "http://images.cocodataset.org/test-stuff2017/000000000019.jpg",
    "http://images.cocodataset.org/test-stuff2017/000000000128.jpg",
    "http://images.cocodataset.org/test-stuff2017/000000000171.jpg",
    "http://images.cocodataset.org/test-stuff2017/000000000184.jpg",
    "http://images.cocodataset.org/test-stuff2017/000000000300.jpg",
    "http://images.cocodataset.org/test-stuff2017/000000000311.jpg",
    "http://images.cocodataset.org/test-stuff2017/000000000333.jpg",
    "http://images.cocodataset.org/test-stuff2017/000000000416.jpg",
]


@serve.deployment
class ImageObjectioner:
    def __init__(self, handle: RayServeHandle, device="cpu"):
        self.model = models.resnet50(pretrained=True)
        self.model.eval().to(device)
        self.device = device
        self.handle = handle

    async def predict(self, uris: List[str]):

        preprocessing_tasks = []
        for uri in uris:
            preprocessing_tasks.append(await self.handle.remote([uri]))
        image_tensors_lists = await asyncio.gather(*preprocessing_tasks)
        image_tensors = [
            tensor for item_tensors in image_tensors_lists for tensor in item_tensors
        ]
        data = torch.cat(image_tensors).to(self.device)
        start = time.time()
        res = self.model(data).to("cpu")
        end = time.time()
        return {"result": res, "model_inference_latency": end - start}


@serve.deployment(
    ray_actor_options={"runtime_env": {"pip": ["validators"]}}, num_replicas=5
)
class DataDownloader:
    def __init__(self):
        self.utils = torch.hub.load(
            "NVIDIA/DeepLearningExamples:torchhub", "nvidia_convnets_processing_utils"
        )

    async def _get_tensor_from_img(self, uri: str):
        return await asyncio.coroutine(self.utils.prepare_input_from_uri)(uri)

    async def __call__(self, uris: List[str]):
        return await asyncio.gather(*[self._get_tensor_from_img(uri) for uri in uris])


async def measure_http_throughput_tps(data_size: int = 8, requests_sent: int = 8):

    tps_stats = []
    model_inference_stats = []

    async def fetch(session):
        async with session.get(
            "http://localhost:8000/", json=input_uris * int(data_size / len(input_uris))
        ) as response:
            return await response.json()

    async with aiohttp.ClientSession() as session:
        for _ in range(requests_sent):
            start = time.time()
            res = await fetch(session)
            end = time.time()
            tps_stats.append(data_size / (end - start))
            model_inference_stats.append(res["model_inference_latency"])

    return tps_stats, model_inference_stats


async def trial(measure_func, data_size: int = 8, num_clients: int = 1):

    client_tasks = [measure_func for _ in range(num_clients)]

    result_stats_list = await asyncio.gather(
        *[client_task(data_size) for client_task in client_tasks]
    )

    throughput_stats_tps = []
    for client_stats in result_stats_list:
        throughput_stats_tps.extend(client_stats[0])
    throuput_mean = round(np.mean(throughput_stats_tps), 2)

    model_inference_latency = []
    for client_stats in result_stats_list:
        model_inference_latency.extend(client_stats[1])
    inference_latency_mean = round(np.mean(model_inference_latency), 2)

    return throuput_mean, inference_latency_mean


async def json_resolver(request: starlette.requests.Request):
    return await request.json()


@click.command()
@click.option("--gpu-env", type=bool, is_flag=True, default=False)
@click.option("--smoke-run", type=bool, is_flag=True, default=False)
def main(gpu_env: Optional[bool], smoke_run: Optional[bool]):

    loop = asyncio.get_event_loop()
    test_name = "resnet50_cpu"
    device = "cpu"
    if gpu_env:
        test_name = "resnet50_gpu"
        device = "cuda"
        ImageObjectioner.set_options(ray_actor_options={"num_gpus": 1})

    # batch size
    batch_sizes = [16, 32, 64]

    with InputNode() as user_input:
        io = ImageObjectioner.bind(DataDownloader.bind(), device=device)
        dag = DAGDriver.bind(io.predict.bind(user_input), http_adapter=json_resolver)
        handle = serve.run(dag)

    if smoke_run:
        res = handle.predict.remote(input_uris)
        print(ray.get(res))

    else:
        result = {}
        print("warming up...")
        for _ in range(10):
            res = handle.predict.remote([input_uris[0]])
        print("start load testing...")
        for batch_size in batch_sizes:
            throughput_mean_tps, model_inference_latency_mean = loop.run_until_complete(
                trial(measure_http_throughput_tps, batch_size)
            )
            result[f"batch size {batch_size}"] = {
                "throughput_mean_tps": throughput_mean_tps,
                "model_inference_latency_mean": model_inference_latency_mean,
            }
            print(throughput_mean_tps, model_inference_latency_mean)

        save_test_results(
            {test_name: result},
            default_output_file="/tmp/serve_resent_benchmark.json",
        )


if __name__ == "__main__":
    main()
