import asyncio
from collections import defaultdict
from unittest import mock

import torch

import ray
import ray.experimental.channel as ray_channel


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
    ray.experimental.channel.torch_tensor_nccl_channel._NcclGroup = MockNcclGroup

    # PyTorch mocks.
    stream_patcher = mock.patch(
        "torch.cuda.current_stream", new_callable=lambda: MockCudaStream
    )
    stream_patcher.start()
    tensor_patcher = mock.patch("torch.Tensor.device", torch.device("cuda"))
    tensor_patcher.start()
    tensor_patcher = mock.patch("torch.Tensor.is_cuda", True)
    tensor_patcher.start()

    ctx = ray_channel.ChannelContext.get_current()
    ctx.set_torch_device(torch.device("cuda"))
