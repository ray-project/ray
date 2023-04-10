from typing import Any

import pytest
import ray
import torch
from ray.experimental.parallel_ml.communicator.naive import NaiveCommunicator
from ray.experimental.parallel_ml.communicator.torch import TorchBasedCommunicator
from ray.experimental.parallel_ml.test.utils import (
    Actor,
    ray_start_4_cpus_2_gpus,
    ray_start_auto,
)
from ray.tests.conftest import *  # noqa


def test_naive_send_receive(ray_start_4_cpus_2_gpus):
    actor0 = Actor.remote(2, 0, NaiveCommunicator)
    actor1 = Actor.remote(2, 1, NaiveCommunicator)
    tensor = torch.tensor([1, 2, 3])
    actor0.send.remote(tensor, 1)
    received = ray.get(actor1.receive.remote(0, tensor.shape, tensor.dtype))
    assert tensor.eq(received).all()


@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires at least 2 GPUs")
def test_torch_send_receive(ray_start_auto):
    actor0 = Actor.options(num_gpus=1).remote(2, 0, TorchBasedCommunicator)
    address = ray.get(actor0.get_master_address.remote())
    print(f"address: {address}")
    actor1 = Actor.options(num_gpus=1).remote(2, 1, TorchBasedCommunicator, address)
    tensor = torch.tensor([1, 2, 3])
    actor0.send.remote(tensor, 1)
    received = ray.get(actor1.receive.remote(0, tensor.shape, tensor.dtype))
    assert tensor.eq(received).all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
