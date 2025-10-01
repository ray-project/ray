import asyncio
from collections import defaultdict
from typing import Optional, Tuple
from unittest import mock

import torch

import ray
import ray.dag
import ray.experimental.channel as ray_channel
from ray.experimental.channel import nccl_group
from ray.experimental.channel.communicator import TorchTensorAllocator
from ray.experimental.util.types import Device


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

        # Add a new mock for the TorchTensorType.device property
        device_property_patcher = mock.patch(
            "ray.experimental.channel.torch_tensor_type.TorchTensorType.device",
            new_callable=mock.PropertyMock,
            return_value=Device.CPU,
        )
        device_property_patcher.start()

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

    def synchronize(self):
        pass


class MockNcclGroup(nccl_group._NcclGroup):
    """
    Mock the internal _NcclGroup to use a barrier actor instead of a NCCL group
    for communication.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # We use the op index to synchronize the sender and receiver at the
        # barrier.
        self.num_ops = defaultdict(int)
        self.barriers = set()

    def send(self, tensor: torch.Tensor, peer_rank: int):
        # "Send" the tensor to the barrier actor.
        barrier_key = sorted([self.get_self_rank(), peer_rank])
        barrier_key = f"barrier-{barrier_key[0]}-{barrier_key[1]}"
        barrier = ray.get_actor(name=barrier_key)
        self.barriers.add(barrier)
        ray.get(barrier.wait.remote(self.num_ops[barrier_key], tensor))
        self.num_ops[barrier_key] += 1

    def recv(
        self,
        shape: Tuple[int],
        dtype: torch.dtype,
        peer_rank: int,
        allocator: Optional[TorchTensorAllocator] = None,
    ):
        # "Receive" the tensor from the barrier actor.
        barrier_key = sorted([self.get_self_rank(), peer_rank])
        barrier_key = f"barrier-{barrier_key[0]}-{barrier_key[1]}"
        barrier = ray.get_actor(name=barrier_key)
        self.barriers.add(barrier)
        received_tensor = ray.get(barrier.wait.remote(self.num_ops[barrier_key]))
        assert (
            allocator is not None
        ), "torch tensor allocator is required for MockNcclGroup"
        buf = allocator(shape, dtype)
        buf[:] = received_tensor[:]
        self.num_ops[barrier_key] += 1
        return buf

    def destroy(self) -> None:
        for barrier in self.barriers:
            ray.kill(barrier)


def start_nccl_mock():
    """
    Patch methods that require CUDA.
    """
    # Mock cupy dependencies.
    nccl_mock = mock.MagicMock()
    nccl_mock.nccl.get_unique_id.return_value = 0
    cp_patcher = mock.patch.dict(
        "sys.modules",
        {
            "cupy.cuda": nccl_mock,
            "cupy": mock.MagicMock(),
            "ray.util.collective.collective_group": mock.MagicMock(),
        },
    )
    cp_patcher.start()

    # Mock send/recv ops to use an actor instead of NCCL.
    ray.experimental.channel.nccl_group._NcclGroup = MockNcclGroup

    # PyTorch mocks.
    stream_patcher = mock.patch(
        "torch.cuda.current_stream", new_callable=lambda: MockCudaStream
    )
    stream_patcher.start()
    new_stream_patcher = mock.patch(
        "torch.cuda.Stream", new_callable=lambda: MockCudaStream
    )
    new_stream_patcher.start()
    tensor_patcher = mock.patch("torch.Tensor.device", torch.device("cuda"))
    tensor_patcher.start()
    tensor_patcher = mock.patch("torch.Tensor.is_cuda", True)
    tensor_patcher.start()
    tensor_allocator_patcher = mock.patch(
        "ray.experimental.channel.torch_tensor_accelerator_channel._torch_zeros_allocator",
        lambda shape, dtype: torch.zeros(shape, dtype=dtype),
    )
    tensor_allocator_patcher.start()

    # Add a new mock for the TorchTensorType.device property
    device_property_patcher = mock.patch(
        "ray.experimental.channel.torch_tensor_type.TorchTensorType.device",
        new_callable=mock.PropertyMock,
        return_value=Device.CPU,
    )
    device_property_patcher.start()

    ctx = ray_channel.ChannelContext.get_current()
    ctx.set_torch_device(torch.device("cuda"))


class TracedChannel(ray_channel.shared_memory_channel.Channel):
    """
    Patched Channel that records all write ops for testing.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.ops = []

    def write(self, *args, **kwargs):
        self.ops.append((args, kwargs))
        return super().write(*args, **kwargs)
