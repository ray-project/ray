"""
Serve Resnet50 model benchmarking.

Including tasks:
1. Image downloading
2. Image convesion to tensors.
3. Batch tensors.
4. Inference with Restnet50 model

Beside last step, all steps are done inside the CPU, and model inference step is
finished on the GPU device.

In the benchmarking, the image download and tensor conversion is done across different
replicas on CPUs.
"""

import os
from typing import List, Optional
import asyncio
import time

import aiohttp
import click
import numpy as np
import starlette.requests
import torch
from torchvision import models

from ray import serve
from ray.serve.handle import DeploymentHandle

from serve_test_utils import save_test_results


# 8 images as input when batch size increase, we replica the input here
input_uris = [
    "https://serve-resnet-benchmark-data.s3.us-west-1.amazonaws.com/000000000019.jpeg",
    "https://serve-resnet-benchmark-data.s3.us-west-1.amazonaws.com/000000000128.jpeg",
    "https://serve-resnet-benchmark-data.s3.us-west-1.amazonaws.com/000000000171.jpeg",
    "https://serve-resnet-benchmark-data.s3.us-west-1.amazonaws.com/000000000184.jpeg",
    "https://serve-resnet-benchmark-data.s3.us-west-1.amazonaws.com/000000000300.jpeg",
    "https://serve-resnet-benchmark-data.s3.us-west-1.amazonaws.com/000000000311.jpeg",
    "https://serve-resnet-benchmark-data.s3.us-west-1.amazonaws.com/000000000333.jpeg",
    "https://serve-resnet-benchmark-data.s3.us-west-1.amazonaws.com/000000000416.jpeg",
]


@serve.deployment
class ImageObjectioner:
    def __init__(self, handle: DeploymentHandle, device="cpu"):
        self.model = models.resnet50(pretrained=True)
        self.model.eval().to(device)
        self.device = device
        self.handle = handle

    async def predict(self, uris: List[str]):

        preprocessing_tasks = []
        for uri in uris:
            preprocessing_tasks.append(self.handle.remote([uri]))
        image_tensors_lists = await asyncio.gather(*preprocessing_tasks)
        image_tensors = [
            tensor for item_tensors in image_tensors_lists for tensor in item_tensors
        ]
        data = torch.cat(image_tensors).to(self.device)
        start = time.time()
        res = self.model(data).to("cpu")
        end = time.time()
        return {"result": res, "model_inference_latency": end - start}

    async def __call__(self, request: starlette.requests.Request):
        uris = await request.json()
        return await self.predict(uris)


@serve.deployment(num_replicas=5)
class DataDownloader:
    def __init__(self):

        # For mutiple process scheduled in same node, torch.hub.load doesn't
        # handle the multi process to dowloand module well. So this is to make sure
        # there is only one replica to download the package
        if os.path.exists("/home/ray/.cache/torch/") is False:
            self.utils = torch.hub.load(
                "NVIDIA/DeepLearningExamples:torchhub",
                "nvidia_convnets_processing_utils",
            )
            with open("/home/ray/.cache/torch/success", "w") as _:
                pass

        else:
            counter = 3
            while counter:
                print("waiting for torch hub NVIDIA package download...")
                time.sleep(20)
                if os.path.exists("/home/ray/.cache/torch/success"):
                    self.utils = torch.hub.load(
                        "NVIDIA/DeepLearningExamples:torchhub",
                        "nvidia_convnets_processing_utils",
                    )
                    break
                counter -= 1
            if counter == 0:
                raise Exception(
                    "Failed to load module nvidia_convnets_processing_utils"
                )

    def __call__(self, uris: List[str]):
        return [self.utils.prepare_input_from_uri(uri) for uri in uris]


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
    throughput_mean = round(np.mean(throughput_stats_tps), 2)

    model_inference_latency = []
    for client_stats in result_stats_list:
        model_inference_latency.extend(client_stats[1])
    inference_latency_mean = round(np.mean(model_inference_latency), 2)

    return throughput_mean, inference_latency_mean


@click.command()
@click.option(
    "--gpu-env",
    type=bool,
    is_flag=True,
    default=False,
    help="If it is set, the model inference will be run on the GPU,"
    "otherwise it is run on CPU",
)
@click.option("--smoke-run", type=bool, is_flag=True, default=False)
def main(gpu_env: Optional[bool], smoke_run: Optional[bool]):

    test_name = "resnet50_cpu"
    device = "cpu"
    if gpu_env:
        test_name = "resnet50_gpu"
        device = "cuda"
        io = ImageObjectioner.options(ray_actor_options={"num_gpus": 1}).bind(
            DataDownloader.bind(), device=device
        )
    else:
        io = ImageObjectioner.bind(DataDownloader.bind(), device=device)

    handle = serve.run(io)

    if smoke_run:
        res = handle.predict.remote(input_uris)
        print(res.result())

    else:
        result = {}
        print("warming up...")
        for _ in range(10):
            handle.predict.remote([input_uris[0]]).result()

        print("start load testing...")
        batch_sizes = [16, 32, 64]
        for batch_size in batch_sizes:
            throughput_mean_tps, model_inference_latency_mean = asyncio.run(
                trial(measure_http_throughput_tps, batch_size)
            )
            result[f"batch size {batch_size}"] = {
                "throughput_mean_tps": throughput_mean_tps,
                "model_inference_latency_mean": model_inference_latency_mean,
            }
            print(throughput_mean_tps, model_inference_latency_mean)

        save_test_results({test_name: result})


if __name__ == "__main__":
    main()
