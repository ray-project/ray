"""Test the send/recv API."""
import pytest
import numpy as np
import ray

from ray.util.collective.types import Backend
from ray.util.collective.tests.cpu_util import create_collective_workers


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("dst_rank", [0, 1])
@pytest.mark.parametrize(
    "array_size", [2, 2**5, 2**10, 2**15, 2**20, [2, 2], [5, 9, 10, 85]])
def test_reduce_different_name(ray_start_single_node, group_name, array_size,
                               dst_rank, backend):
    world_size = 2
    actors, _ = create_collective_workers(
        num_workers=world_size, group_name=group_name, backend=backend)
    ray.wait([
        a.set_buffer.remote(np.ones(array_size, dtype=np.float32) * (i + 1))
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
        assert (results[i] == np.ones(array_size, dtype=np.float32) *
                (src_rank + 1)).all()


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("dst_rank", [0, 1])
def test_sendrecv_torch_numpy(ray_start_single_node, dst_rank, backend):
    import torch
    world_size = 2
    actors, _ = create_collective_workers(world_size, backend=backend)
    ray.wait([actors[1].set_buffer.remote(torch.ones(10, ) * 2)])
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
        assert (results[0] == np.ones((10, )) * 2).all()
        assert (results[1] == torch.ones((10, )) * 2).all()
    else:
        assert (results[0] == np.ones((10, ))).all()
        assert (results[1] == torch.ones((10, ))).all()


@pytest.mark.parametrize("backend", [Backend.GLOO])
def test_sendrecv_invalid_rank(ray_start_single_node, backend, dst_rank=3):
    world_size = 2
    actors, _ = create_collective_workers(world_size, backend=backend)
    with pytest.raises(ValueError):
        ray.get([a.do_send.remote(dst_rank=dst_rank) for a in actors])


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", "-x", __file__]))
