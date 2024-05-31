# coding: utf-8
import logging
import os
import sys
import torch
import mock
import asyncio
from collections import defaultdict

import pytest

import ray
import ray.cluster_utils
import ray.experimental.channel as ray_channel
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.tests.conftest import *  # noqa
from ray.dag import InputNode

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
class MockedWorker:
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

        comm_patcher = mock.patch("cupy.cuda.nccl.NcclCommunicator")
        comm_patcher.start()

        ray.experimental.channel.torch_tensor_nccl_channel._NcclGroup = MockNcclGroup

        tensor_patcher = mock.patch("torch.Tensor.device", torch.device("cuda"))
        tensor_patcher.start()
        tensor_patcher = mock.patch("torch.Tensor.is_cuda", True)
        tensor_patcher.start()
        tensor_patcher = mock.patch(
            "ray.experimental.channel.torch_tensor_nccl_channel._torch_zeros_allocator",
            lambda meta: torch.zeros(meta.shape, dtype=meta.dtype),
        )
        tensor_patcher.start()

        ctx = ray_channel.ChannelContext.get_current()
        ctx.set_torch_device(torch.device("cuda"))

    def set_nccl_channel(self, typ, chan):
        typ.register_custom_serializer()
        self.chan = chan

    def create_nccl_channel(self, typ: TorchTensorType, readers):
        typ.register_custom_serializer()
        self.chan = typ.create_channel(
            ray.get_runtime_context().current_actor,
            readers,
            _torch_tensor_allocator=lambda meta: torch.zeros(
                meta.shape, dtype=meta.dtype
            ),
        )

        return self.chan

    def send(self, shape, dtype, value: int):
        return torch.ones(shape, dtype=dtype) * value

    def recv(self, tensor):
        return (tensor[0].item(), tensor.shape, tensor.dtype)


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

    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        # TODO(swang): Test that we are using the minimum number of
        # channels/messages when direct_return=True.
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        output_channel = compiled_dag.execute(i)
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == (i, shape, dtype)
        output_channel.end_read()

    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
