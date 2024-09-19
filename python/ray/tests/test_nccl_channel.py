# coding: utf-8
import logging
import os
import sys
import torch
from typing import List, Tuple

import pytest

import ray
import ray.cluster_utils
from ray.experimental.channel.conftest import (
    Barrier,
    start_nccl_mock,
)
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.experimental.channel.torch_tensor_nccl_channel import (
    _init_nccl_group,
)
from ray._private.test_utils import get_actor_node_id

logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0, num_gpus=1)
class Worker:
    def __init__(self):
        self.chan = None

    def start_mock(self):
        start_nccl_mock()

    def set_nccl_channel(self, typ, chan):
        typ.register_custom_serializer()
        self.chan = chan

    def create_nccl_channel(
        self,
        typ: TorchTensorType,
        reader_and_node_list: List[Tuple[ray.actor.ActorHandle, str]],
    ):
        typ.register_custom_serializer()
        self.chan = typ.create_channel(
            ray.get_runtime_context().current_actor,
            reader_and_node_list,
            False,
            _torch_tensor_allocator=lambda shape, dtype: torch.zeros(
                shape, dtype=dtype
            ),
        )

        return self.chan

    def produce(self, val: int, shape: Tuple[int], dtype: torch.dtype):
        t = torch.ones(shape, dtype=dtype) * val
        self.chan.write(t)

    def receive(self):
        t = self.chan.read()
        data = (t[0].clone(), t.shape, t.dtype)
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
    receiver_node = get_actor_node_id(receiver)

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
    chan_ref = sender.create_nccl_channel.remote(chan_typ, [(receiver, receiver_node)])
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
    receiver_to_node = []
    for _ in range(3):
        handle = Worker.remote()
        node = get_actor_node_id(handle)
        receiver_to_node.append((handle, node))

    workers = [sender] + [receiver for receiver, _ in receiver_to_node]

    ray.get([worker.start_mock.remote() for worker in workers])

    nccl_id = _init_nccl_group(workers)

    chan_typ = TorchTensorType(
        transport="nccl",
    )
    chan_typ.set_nccl_group_id(nccl_id)
    chan_ref = sender.create_nccl_channel.remote(chan_typ, receiver_to_node)
    receiver_ready = [
        receiver.set_nccl_channel.remote(chan_typ, chan_ref)
        for receiver, _ in receiver_to_node
    ]
    ray.get(receiver_ready)

    shape = (10,)
    dtype = torch.float16

    all_refs = []
    for i in range(2):
        sender.produce.remote(i, shape, dtype)
        all_refs.append([receiver.receive.remote() for receiver, _ in receiver_to_node])
    # Check that all receivers received the correct value.
    for i, refs in enumerate(all_refs):
        for val in ray.get(refs):
            assert val == (i, shape, dtype)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
