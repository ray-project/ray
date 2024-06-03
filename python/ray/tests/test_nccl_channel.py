# coding: utf-8
import logging
import os
import sys
import torch
import mock
import asyncio
from collections import defaultdict
from typing import List, Tuple

import pytest

import ray
import ray.cluster_utils
import ray.experimental.channel as ray_channel
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.experimental.channel.torch_tensor_nccl_channel import (
    _init_nccl_group,
)

logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0)
class Barrier:
    """
    Barrier that blocks the given number of actors until all actors have
    reached the barrier. This is used to mock out blocking NCCL ops.
    """

    def __init__(self, num_actors=2):
        self.num_actors = num_actors
        self.condition = asyncio.Condition()
        # Buffer for the data that is "sent" between the actors, each entry is
        # one p2p op.
        self.data = {}
        # Buffer for the number of actors seen, each entry is one p2p op.
        self.num_actors_seen = defaultdict(int)

    async def wait(self, idx: int, data=None):
        """
        Wait at barrier until all actors have sent `idx`. One actor should
        provide `data`, and this value will be returned by this method for all
        other actors.
        """
        async with self.condition:
            if data is not None:
                assert idx not in self.data, (self.data, self.num_actors_seen)
                self.data[idx] = data
            self.num_actors_seen[idx] += 1

            if self.num_actors_seen[idx] == self.num_actors:
                # Wake up all tasks waiting on this condition.
                self.condition.notify_all()
            else:
                await self.condition.wait_for(
                    lambda: self.num_actors_seen[idx] == self.num_actors
                )

            if data is None:
                data = self.data[idx]

        return data


class MockCudaStream:
    def __init__(self):
        self.cuda_stream = 0


class MockNcclGroup(ray_channel.nccl_group._NcclGroup):
    """
    Mock the internal _NcclGroup to use a barrier actor instead of a NCCL group
    for communication.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # We use the op index to synchronize the sender and receiver at the
        # barrier.
        self.num_ops = defaultdict(int)

    def send(self, tensor: torch.Tensor, peer_rank: int):
        # "Send" the tensor to the barrier actor.
        barrier_key = f"barrier-{self.get_self_rank()}-{peer_rank}"
        barrier = ray.get_actor(name=barrier_key)
        ray.get(barrier.wait.remote(self.num_ops[barrier_key], tensor))
        self.num_ops[barrier_key] += 1

    def recv(self, buf: torch.Tensor, peer_rank: int):
        # "Receive" the tensor from the barrier actor.
        barrier_key = f"barrier-{peer_rank}-{self.get_self_rank()}"
        barrier = ray.get_actor(name=barrier_key)
        received_tensor = ray.get(barrier.wait.remote(self.num_ops[barrier_key]))
        buf[:] = received_tensor[:]
        self.num_ops[barrier_key] += 1


@ray.remote(num_cpus=0, num_gpus=1)
class Worker:
    def __init__(self):
        self.chan = None

    def start_mock(self):
        """
        Patch methods that require CUDA.
        """
        stream_patcher = mock.patch(
            "torch.cuda.current_stream", new_callable=lambda: MockCudaStream
        )
        stream_patcher.start()

        cp_stream_patcher = mock.patch("cupy.cuda.ExternalStream")
        cp_stream_patcher.start()

        comm_patcher = mock.patch(
            "ray.util.collective.collective_group.nccl_util.NcclCommunicator"
        )
        comm_patcher.start()

        ray.experimental.channel.torch_tensor_nccl_channel._NcclGroup = MockNcclGroup

        tensor_patcher = mock.patch("torch.Tensor.device", torch.device("cuda"))
        tensor_patcher.start()
        tensor_patcher = mock.patch("torch.Tensor.is_cuda", True)
        tensor_patcher.start()

        ctx = ray_channel.ChannelContext.get_current()
        ctx.set_torch_device(torch.device("cuda"))

    def set_nccl_channel(self, typ, chan):
        typ.register_custom_serializer()
        self.chan = chan

    def create_nccl_channel(
        self, typ: TorchTensorType, readers: List[ray.actor.ActorHandle]
    ):
        typ.register_custom_serializer()
        self.chan = typ.create_channel(
            ray.get_runtime_context().current_actor,
            readers,
            _torch_tensor_allocator=lambda shape, dtype: torch.zeros(
                shape, dtype=dtype
            ),
        )

        return self.chan

    def produce(self, val: int, shape: Tuple[int], dtype: torch.dtype):
        t = torch.ones(shape, dtype=dtype) * val
        self.chan.write(t)

    def receive(self):
        t = self.chan.begin_read()
        data = (t[0].clone(), t.shape, t.dtype)
        self.chan.end_read()
        return data


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 2,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_p2p(ray_start_cluster):
    """
    Test simple sender -> receiver pattern. Check that receiver receives
    correct results.
    """
    # Barrier name should be barrier-{sender rank}-{receiver rank}.
    barrier = Barrier.options(name="barrier-0-1").remote()  # noqa

    sender = Worker.remote()
    receiver = Worker.remote()

    ray.get(
        [
            sender.start_mock.remote(),
            receiver.start_mock.remote(),
        ]
    )

    nccl_id = _init_nccl_group([sender, receiver])

    chan_typ = TorchTensorType(
        transport="nccl",
    )
    chan_typ.set_nccl_group_id(nccl_id)
    chan_ref = sender.create_nccl_channel.remote(chan_typ, [receiver])
    receiver_ready = receiver.set_nccl_channel.remote(chan_typ, chan_ref)
    ray.get([chan_ref, receiver_ready])

    shape = (10,)
    dtype = torch.float16

    refs = []
    for i in range(3):
        sender.produce.remote(i, shape, dtype)
        refs.append(receiver.receive.remote())
    assert ray.get(refs) == [(i, shape, dtype) for i in range(3)]


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 4,
            "num_gpus": 4,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_multiple_receivers(ray_start_cluster):
    """
    Test sender with multiple receivers pattern. Check that all receivers
    receive correct results.
    """
    # Create one barrier per sender-receiver pair.
    barriers = [  # noqa
        Barrier.options(name=f"barrier-0-{i}").remote() for i in range(1, 4)
    ]  # noqa

    sender = Worker.remote()
    receivers = [Worker.remote() for _ in range(3)]
    workers = [sender] + receivers

    ray.get([worker.start_mock.remote() for worker in workers])

    nccl_id = _init_nccl_group(workers)

    chan_typ = TorchTensorType(
        transport="nccl",
    )
    chan_typ.set_nccl_group_id(nccl_id)
    chan_ref = sender.create_nccl_channel.remote(chan_typ, receivers)
    receiver_ready = [
        receiver.set_nccl_channel.remote(chan_typ, chan_ref) for receiver in receivers
    ]
    ray.get(receiver_ready)

    shape = (10,)
    dtype = torch.float16

    all_refs = []
    for i in range(2):
        sender.produce.remote(i, shape, dtype)
        all_refs.append([receiver.receive.remote() for receiver in receivers])
    # Check that all receivers received the correct value.
    for i, refs in enumerate(all_refs):
        for val in ray.get(refs):
            assert val == (i, shape, dtype)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
