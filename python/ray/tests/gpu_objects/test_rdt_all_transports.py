import os
import sys

import pytest
import torch

import ray
from ray.experimental.collective import create_collective_group

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))
TRANSPORTS_AND_DEVICES = (
    [("nixl", "cuda"), ("nccl", "cuda"), ("gloo", "cpu")]
    if USE_GPU
    else [("gloo", "cpu")]
)


@ray.remote(num_cpus=0, num_gpus=1 if USE_GPU else 0, enable_tensor_transport=True)
class AsyncActor:
    async def send(self, data, device):
        device_data = data.to(device)
        return device_data

    async def intermediate(self, device_data):
        return device_data

    async def recv(self, device_data):
        return device_data


@pytest.mark.parametrize(
    "ray_start_regular_shared", [{"num_gpus": 4} if USE_GPU else {}], indirect=True
)
@pytest.mark.parametrize("transport, device", TRANSPORTS_AND_DEVICES)
def test_rdt_async_chain(ray_start_regular_shared, transport, device):
    actors = [AsyncActor.remote() for _ in range(3)]
    if transport == "gloo" or transport == "nccl":
        create_collective_group(actors, transport)
    data = torch.randn(100, 100)
    send_ref = actors[0].send.options(tensor_transport=transport).remote(data, device)
    int_ref = (
        actors[1].intermediate.options(tensor_transport=transport).remote(send_ref)
    )
    recv_ref = actors[2].recv.remote(int_ref)
    data = ray.get(recv_ref)
    assert data.device.type == device


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
