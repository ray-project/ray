import os
import sys

import pytest

import ray
from ray.experimental.channel.gloo_group import _init_gloo_group
from ray.experimental.channel import ChannelContext
import numpy as np
from ray.experimental.channel.gloo_channel import _destroy_gloo_group


@ray.remote
class Worker:
    def __init__(self, rank: int, group_name: str):
        self._rank = rank
        self._group_name = group_name

    def send(self, buf, peer_rank: int):
        ctx = ChannelContext.get_current()
        group = ctx.gloo_groups[self._group_name]
        group.send(buf, peer_rank)

    def recv(self, buf, peer_rank: int):
        ctx = ChannelContext.get_current()
        group = ctx.gloo_groups[self._group_name]
        group.recv(buf, peer_rank)
        return buf


def test_gloo_group_2_workers():
    gloo_group_name = "test_group"
    actor_handles = [Worker.remote(rank, gloo_group_name) for rank in range(2)]
    _init_gloo_group(gloo_group_name, actor_handles)

    ctx = ChannelContext.get_current()
    gloo_group = ctx.gloo_groups[gloo_group_name]

    assert gloo_group.get_rank(actor_handles[0]) == 0
    assert gloo_group.get_rank(actor_handles[1]) == 1

    send_buf = np.random.rand(100).astype(np.float32)
    recv_buf = np.empty_like(send_buf)

    res = ray.get(
        [
            actor_handles[0].send.remote(send_buf, 1),
            actor_handles[1].recv.remote(recv_buf, 0),
        ]
    )
    assert np.all(send_buf == res[1])


def test_gloo_group_3_workers():
    gloo_group_name = "test_group"
    actor_handles = [Worker.remote(rank, gloo_group_name) for rank in range(3)]
    _init_gloo_group(gloo_group_name, actor_handles)

    ctx = ChannelContext.get_current()
    gloo_group = ctx.gloo_groups[gloo_group_name]

    assert gloo_group.get_rank(actor_handles[0]) == 0
    assert gloo_group.get_rank(actor_handles[1]) == 1
    assert gloo_group.get_rank(actor_handles[2]) == 2

    def send_and_recv(send_rank, recv_rank):
        send_buf = np.random.rand(100).astype(np.float32)
        recv_buf = np.empty_like(send_buf)
        res = ray.get(
            [
                actor_handles[send_rank].send.remote(send_buf, recv_rank),
                actor_handles[recv_rank].recv.remote(recv_buf, send_rank),
            ]
        )
        assert np.all(send_buf == res[1])

    send_and_recv(0, 1)
    send_and_recv(1, 0)
    send_and_recv(1, 2)
    send_and_recv(2, 1)
    send_and_recv(2, 0)
    send_and_recv(0, 2)

    _destroy_gloo_group(gloo_group_name)


def test_gloo_group_send_diff_shape():
    gloo_group_name = "test_group"
    actor_handles = [Worker.remote(rank, gloo_group_name) for rank in range(2)]
    _init_gloo_group(gloo_group_name, actor_handles)

    send_buf = np.ones((10,), dtype=np.float32)
    recv_buf = np.empty_like(send_buf)
    res = ray.get(
        [
            actor_handles[0].send.remote(send_buf, 1),
            actor_handles[1].recv.remote(recv_buf, 0),
        ]
    )
    assert np.all(send_buf == res[1])

    send_buf = np.ones((20,), dtype=np.float32)
    recv_buf = np.empty_like(send_buf)
    res = ray.get(
        [
            actor_handles[0].send.remote(send_buf, 1),
            actor_handles[1].recv.remote(recv_buf, 0),
        ]
    )
    assert np.all(send_buf == res[1])

    _destroy_gloo_group(gloo_group_name)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
