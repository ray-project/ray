import os
import sys
import torch
from typing import List, Dict, Tuple, Optional

import pytest

import ray
import ray.cluster_utils
import ray.experimental.collective as collective
import time
from ray.exceptions import RayChannelError
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.experimental.channel.conftest import start_nccl_mock
from ray.experimental.channel.cpu_nccl_group import CPUNcclGroup
from ray.experimental.channel.gpu_communicator import TorchTensorAllocator, ReduceOp, GPUCommunicator
from ray.tests.conftest import *  # noqa

class DefaultTensorAllocator(TorchTensorAllocator):
    def __call__(self, shape: Tuple[int, ...], dtype: torch.dtype) -> torch.Tensor:
        return torch.empty(shape, dtype=dtype)

@ray.remote(num_cpus=1, num_gpus=0)
class Worker:
    def __init__(self, rank):
        self.nccl_group = None
        self.rank = rank
        self.allocator = DefaultTensorAllocator()

    def set_nccl_channel(self, nccl_group):
        self.nccl_group = nccl_group

    def send(self, val, shape, dtype, peer_rank):
        try:
            t = torch.ones(shape, dtype=dtype) * val
            self.nccl_group.send(t, peer_rank)
            return True
        except RayChannelError as e:
            print(f"Send error: {e}")
            return False

    def receive(self, shape, dtype, peer_rank):
        try:
            t = self.nccl_group.recv(
                shape=shape,
                dtype=dtype,
                peer_rank=peer_rank,
                allocator=self.allocator
            )
            return (t[0].item(), t.shape, t.dtype)
        except RayChannelError as e:
            print(f"Receive error: {e}")
            return None

    def allreduce(self, val, shape, dtype):
        t = torch.ones(shape, dtype=dtype) * val
        recv_t = torch.zeros(shape, dtype=dtype)
        try:
            t = self.nccl_group.allreduce(
                send_buf=t,
                recv_buf=recv_t,
            )
            return (recv_t[0].item(), recv_t.shape, recv_t.dtype)
        except RayChannelError as e:
            print(f"Allreduce error: {e}")
            return None

# @pytest.mark.parametrize(
#     "ray_start_cluster",
#     [
#         {
#             "num_cpus": 2,
#             "num_gpus": 0,
#             "num_nodes": 1,
#         }
#     ],
#     indirect=True,
# )
# def test_cpu_p2p(ray_start_cluster):
#     sender = Worker.remote(rank=0)
#     receiver = Worker.remote(rank=1)

#     nccl_group = CPUNcclGroup(
#         world_size=2,
#         rank=0,
#         actor_handles=[sender, receiver]
#     )
#     r_nccl_group = CPUNcclGroup(
#         world_size=2,
#         rank=1,
#         actor_handles=[sender, receiver]
#     )

#     ray.get([
#         sender.set_nccl_channel.remote(nccl_group),
#         receiver.set_nccl_channel.remote(r_nccl_group)
#     ])

#     shape = (3,)
#     dtype = torch.float32
#     test_value = 2.0

#     send_future = sender.send.remote(test_value, shape, dtype, peer_rank=1)
#     time.sleep(1)
#     receive_future = receiver.receive.remote(shape, dtype, peer_rank=0)
    
#     send_result = ray.get(send_future)
#     receive_result = ray.get(receive_future)
    
#     received_value, received_shape, received_dtype = receive_result
#     assert received_value == test_value, f"Expected value {test_value}, got {received_value}"

#     nccl_group.destroy()
#     r_nccl_group.destroy()

@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 0,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_cpu_allreduce(ray_start_cluster):
    world_size = 2

    worker1 = Worker.remote(rank=0)
    worker2 = Worker.remote(rank=1)

    nccl_group_1 = CPUNcclGroup(
        world_size=2,
        rank=0,
        actor_handles=[worker1, worker2]
    )
    nccl_group_2 = CPUNcclGroup(
        world_size=2,
        rank=1,
        actor_handles=[worker1, worker2]
    )

    ray.get([
        worker1.set_nccl_channel.remote(nccl_group_1),
        worker2.set_nccl_channel.remote(nccl_group_2)
    ])

    shape = (3,)
    dtype = torch.float32
    test_value = 2.0

    res_ref = [
        worker1.allreduce.remote(test_value, shape, dtype),
        worker2.allreduce.remote(test_value, shape, dtype),
    ]

    res = ray.get(res_ref)

    received_value, received_shape, received_dtype = res[0]
    assert received_value == test_value*2, f"Expected value {test_value}, got {received_value}"

if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
