"""Test the send/recv API."""
import pytest
import cupy as cp
import ray

from ray.util.collective.tests.util import create_collective_workers


@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("dst_rank", [0, 1])
@pytest.mark.parametrize(
    "array_size", [2, 2**5, 2**10, 2**15, 2**20, [2, 2], [5, 9, 10, 85]])
def test_reduce_different_name(ray_start_single_node_2_gpus, group_name,
                               array_size, dst_rank):
    world_size = 2
    actors, _ = create_collective_workers(
        num_workers=world_size, group_name=group_name)
    ray.wait([
        a.set_buffer.remote(cp.ones(array_size, dtype=cp.float32) * (i + 1))
        for i, a in enumerate(actors)
    ])
    src_rank = 1 - dst_rank
    refs = []
    for i, actor in enumerate(actors):
        if i != dst_rank:
            ref = actor.do_send.remote(group_name, dst_rank)
        else:
            ref = actor.do_recv.remote(group_name, src_rank)
        refs.append(ref)
    results = ray.get(refs)
    for i in range(world_size):
        assert (results[i] == cp.ones(array_size, dtype=cp.float32) *
                (src_rank + 1)).all()


@pytest.mark.parametrize("dst_rank", [0, 1])
def test_sendrecv_torch_cupy(ray_start_single_node_2_gpus, dst_rank):
    import torch
    world_size = 2
    actors, _ = create_collective_workers(world_size)
    ray.wait([actors[1].set_buffer.remote(torch.ones(10, ).cuda() * 2)])
    src_rank = 1 - dst_rank

    refs = []
    for i, actor in enumerate(actors):
        if i != dst_rank:
            ref = actor.do_send.remote(dst_rank=dst_rank)
        else:
            ref = actor.do_recv.remote(src_rank=src_rank)
        refs.append(ref)
    results = ray.get(refs)
    if dst_rank == 0:
        assert (results[0] == cp.ones((10, )) * 2).all()
        assert (results[1] == torch.ones((10, )).cuda() * 2).all()
    else:
        assert (results[0] == cp.ones((10, ))).all()
        assert (results[1] == torch.ones((10, )).cuda()).all()


def test_sendrecv_invalid_rank(ray_start_single_node_2_gpus, dst_rank=3):
    world_size = 2
    actors, _ = create_collective_workers(world_size)
    with pytest.raises(ValueError):
        _ = ray.get([a.do_send.remote(dst_rank=dst_rank) for a in actors])
