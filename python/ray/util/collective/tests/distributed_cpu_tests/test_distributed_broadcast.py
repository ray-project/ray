"""Test the broadcast API."""
import pytest
import numpy as np
import ray

from ray.util.collective.types import Backend
from ray.util.collective.tests.cpu_util import create_collective_workers


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("src_rank", [0, 2, 5, 6, 7])
def test_broadcast_different_name(
    ray_start_distributed_2_nodes, group_name, src_rank, backend
):
    world_size = 8
    actors, _ = create_collective_workers(
        num_workers=world_size, group_name=group_name, backend=backend
    )
    ray.wait(
        [
            a.set_buffer.remote(np.ones((10,), dtype=np.float32) * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get(
        [
            a.do_broadcast.remote(group_name=group_name, src_rank=src_rank)
            for a in actors
        ]
    )
    for i in range(world_size):
        assert (results[i] == np.ones((10,), dtype=np.float32) * (src_rank + 2)).all()


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("array_size", [2, 2 ** 5, 2 ** 10, 2 ** 15, 2 ** 20])
@pytest.mark.parametrize("src_rank", [0, 2, 5, 6, 7])
def test_broadcast_different_array_size(
    ray_start_distributed_2_nodes, array_size, src_rank, backend
):
    world_size = 8
    actors, _ = create_collective_workers(world_size, backend=backend)
    ray.wait(
        [
            a.set_buffer.remote(np.ones(array_size, dtype=np.float32) * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get([a.do_broadcast.remote(src_rank=src_rank) for a in actors])
    for i in range(world_size):
        assert (
            results[i] == np.ones((array_size,), dtype=np.float32) * (src_rank + 2)
        ).all()


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("src_rank", [0, 2, 5, 6, 7])
def test_broadcast_torch_numpy(ray_start_distributed_2_nodes, src_rank, backend):
    import torch

    world_size = 8
    actors, _ = create_collective_workers(world_size, backend=backend)
    ray.wait(
        [
            actors[1].set_buffer.remote(
                torch.ones(
                    10,
                )
                * world_size
            )
        ]
    )
    results = ray.get([a.do_broadcast.remote(src_rank=src_rank) for a in actors])
    if src_rank == 0:
        assert (results[0] == np.ones((10,))).all()
        assert (results[1] == torch.ones((10,))).all()
    else:
        assert (results[0] == np.ones((10,)) * world_size).all()
        assert (results[1] == torch.ones((10,)) * world_size).all()


@pytest.mark.parametrize("backend", [Backend.GLOO])
def test_broadcast_invalid_rank(ray_start_distributed_2_nodes, backend, src_rank=9):
    world_size = 8
    actors, _ = create_collective_workers(world_size, backend=backend)
    with pytest.raises(ValueError):
        _ = ray.get([a.do_broadcast.remote(src_rank=src_rank) for a in actors])


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
