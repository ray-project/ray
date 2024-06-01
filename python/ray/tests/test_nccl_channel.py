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
import ray.experimental.channel as channel
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


class MockNcclGroup(channel.nccl_group._NcclGroup):
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


class TracedChannel(channel.shared_memory_channel.Channel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.ops = []

    def write(self, *args, **kwargs):
        self.ops.append((args, kwargs))
        return super().write(*args, **kwargs)


@ray.remote(num_cpus=0, num_gpus=1)
class Worker:
    def __init__(self):
        self.tensor_chan = None
        self.traced_channels = {}

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
        tensor_patcher = mock.patch("torch.Tensor.device", torch.device("cuda"))
        tensor_patcher.start()

        ctx = channel.ChannelContext.get_current()
        ctx.set_torch_device(torch.device("cuda"))

    def set_nccl_channel(self, typ, tensor_chan):
        typ.register_custom_serializer()
        self.tensor_chan = tensor_chan

    def create_nccl_channel(
        self,
        typ: TorchTensorType,
        readers,
        tensor_metadata_channel_key=None,
        non_tensor_data_channel_key=None,
    ):
        typ.register_custom_serializer()

        tensor_metadata_channel = None
        if tensor_metadata_channel_key is not None:
            tensor_metadata_channel = self.traced_channels[tensor_metadata_channel_key]
        non_tensor_data_channel = None
        if non_tensor_data_channel_key is not None:
            non_tensor_data_channel = self.traced_channels[non_tensor_data_channel_key]

        self.tensor_chan = typ.create_channel(
            ray.get_runtime_context().current_actor,
            readers,
            _tensor_metadata_channel=tensor_metadata_channel,
            _non_tensor_data_channel=non_tensor_data_channel,
            _torch_tensor_allocator=lambda meta: torch.zeros(
                meta.shape, dtype=meta.dtype
            ),
        )

        return self.tensor_chan

    def send(self, val, shape, dtype):
        t = torch.ones(shape, dtype=dtype) * val
        self.tensor_chan.write(t)

    def receive(self):
        t = self.tensor_chan.begin_read()
        data = (t[0].clone(), t.shape, t.dtype)
        self.tensor_chan.end_read()
        return data

    def send_dict(self, tensor_dict):
        self.tensor_chan.write(tensor_dict)

    def receive_dict(self):
        tensor_dict = self.tensor_chan.begin_read()
        vals = []
        for key, t in tensor_dict.items():
            vals.append((key, t[0].clone(), t.shape, t.dtype))
        return vals

    def create_traced_channel(self, key, readers):
        self.traced_channels[key] = TracedChannel(
            ray.get_runtime_context().current_actor, readers
        )

    def get_num_channel_ops(self, key):
        ops = self.traced_channels[key].ops
        self.traced_channels[key].ops = []
        return len(ops)


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
        sender.send.remote(i, shape, dtype)
        refs.append(receiver.receive.remote())
    assert ray.get(refs) == [(i, shape, dtype) for i in range(3)]

    shapes = [(10,), (20,), (30,)]
    dtypes = [torch.float, torch.float16, torch.int]

    refs = []
    for i in range(3):
        sender.send.remote(i, shapes[i], dtypes[i])
        refs.append(receiver.receive.remote())
    assert ray.get(refs) == [(i, shapes[i], dtypes[i]) for i in range(3)]


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

    shapes = [(10,), (20,), (30,)]
    dtypes = [torch.float, torch.float16, torch.int]

    all_refs = []
    for i in range(3):
        sender.send.remote(i, shapes[i], dtypes[i])
        all_refs.append([receiver.receive.remote() for receiver in receivers])
    # Check that all receivers received the correct value.
    for i, refs in enumerate(all_refs):
        for val in ray.get(refs):
            assert val == (i, shapes[i], dtypes[i])


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
def test_static_shape(ray_start_cluster):
    """
    Test that when static_shape=True is passed, we only send metadata for the
    first operation. Afterwards, we reuse the same shape and dtype for future
    tensors. Sending a tensor of the wrong shape or dtype throws an error.
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
        static_shape=True,
    )
    chan_typ.set_nccl_group_id(nccl_id)
    sender.create_traced_channel.remote("tensor_metadata", [receiver])
    sender.create_traced_channel.remote("non_tensor_data", [receiver])
    chan_ref = sender.create_nccl_channel.remote(
        chan_typ,
        [receiver],
        "tensor_metadata",
        "non_tensor_data",
    )
    receiver_ready = receiver.set_nccl_channel.remote(chan_typ, chan_ref)
    ray.get([chan_ref, receiver_ready])

    shape = (10,)
    dtype = torch.float16

    refs = []
    for i in range(3):
        sender.send.remote(i, shape, dtype)
        refs.append(receiver.receive.remote())
    assert ray.get(refs) == [(i, shape, dtype) for i in range(3)]

    # When static_shape=True, we should only send one metadata op. After the
    # first metadata is sent, we reuse it for all future sends.
    num_tensor_metadata_ops = ray.get(
        sender.get_num_channel_ops.remote("tensor_metadata")
    )
    assert num_tensor_metadata_ops == 1
    num_non_tensor_data_ops = ray.get(
        sender.get_num_channel_ops.remote("non_tensor_data")
    )
    assert num_non_tensor_data_ops == 3

    # Attempting to write tensors of the wrong shape or dtype will error.
    with pytest.raises(ValueError):
        ray.get(sender.send.remote(4, (20,), dtype))
    with pytest.raises(ValueError):
        ray.get(sender.send.remote(4, shape, torch.float32))

    # The channel is still usable for valid tensors after errors occur.
    sender.send.remote(4, shape, dtype)
    ray.get(receiver.receive.remote()) == (4, shape, dtype)


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
def test_static_non_tensor_data(ray_start_cluster):
    """
    Test that when static_shape=True is passed, we only send metadata for the
    first operation. Afterwards, we reuse the same shape and dtype for future
    tensors. Sending a tensor of the wrong shape or dtype throws an error.
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
        static_non_tensor_data=True,
    )
    chan_typ.set_nccl_group_id(nccl_id)
    sender.create_traced_channel.remote("tensor_metadata", [receiver])
    sender.create_traced_channel.remote("non_tensor_data", [receiver])
    chan_ref = sender.create_nccl_channel.remote(
        chan_typ,
        [receiver],
        "tensor_metadata",
        "non_tensor_data",
    )
    receiver_ready = receiver.set_nccl_channel.remote(chan_typ, chan_ref)
    ray.get([chan_ref, receiver_ready])

    key = "my_tensor"
    shape = 10
    dtype = torch.float16

    # Sending tensors of different shapes is okay as long as the non-tensor
    # data stays the same.
    refs = []
    values = []
    for i in range(1, 4):
        t = torch.ones(i * shape, dtype=dtype) * i
        val = {
            key: t,
        }
        sender.send_dict.remote(val)

        values.append([(key, t[0], t.shape, t.dtype)])
        refs.append(receiver.receive_dict.remote())
    assert ray.get(refs) == values

    # When static_non_tensor_data=True, we should only send one non-tensor data
    # op. After the first data is sent, we reuse it for all future sends.
    num_tensor_metadata_ops = ray.get(
        sender.get_num_channel_ops.remote("tensor_metadata")
    )
    assert num_tensor_metadata_ops == 3
    num_non_tensor_data_ops = ray.get(
        sender.get_num_channel_ops.remote("non_tensor_data")
    )
    assert num_non_tensor_data_ops == 1

    # Attempting to write a different number of tensors will error.
    with pytest.raises(ValueError):
        ray.get(sender.send_dict.remote({}))
    with pytest.raises(ValueError):
        ray.get(
            sender.send_dict.remote(
                {
                    "first": torch.zeros(10),
                    "second": torch.zeros(10),
                }
            )
        )

    # NOTE(swang): Sending values with different non-tensor data, e.g., a list
    # of tensors vs a dictionary of tensors, should throw an error.
    # Unfortunately ray.cloudpickle serialization is not deterministic.
    # Therefore, we cannot detect this case without throwing many false
    # positives.
    # with pytest.raises(ValueError):
    #     sender.send.remote(4, shape, torch.float32)

    # The channel is still usable for valid tensors after errors occur.
    val = {
        key: torch.ones(shape, dtype=dtype),
    }
    sender.send_dict.remote(val)
    assert ray.get(receiver.receive_dict.remote()) == [
        (key, val[key][0], val[key].shape, val[key].dtype)
    ]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
