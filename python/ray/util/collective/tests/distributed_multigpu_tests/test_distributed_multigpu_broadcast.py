"""Test the broadcast API."""
import pytest
import cupy as cp
import ray

from ray.util.collective.tests.util import create_collective_multigpu_workers


@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("src_rank", [0, 1])
@pytest.mark.parametrize("src_gpu_index", [0, 1])
def test_broadcast_different_name(
    ray_start_distributed_multigpu_2_nodes_4_gpus, group_name, src_rank, src_gpu_index
):
    world_size = 2
    num_gpu_per_worker = 2
    actors, _ = create_collective_multigpu_workers(
        num_workers=world_size, group_name=group_name
    )
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([10], value0=4, value1=5))

    results = ray.get(
        [
            a.do_broadcast_multigpu.remote(
                group_name=group_name, src_rank=src_rank, src_gpu_index=src_gpu_index
            )
            for a in actors
        ]
    )
    for i in range(world_size):
        for j in range(num_gpu_per_worker):
            val = (src_rank + 1) * 2 + src_gpu_index
            assert (results[i][j] == cp.ones([10], dtype=cp.float32) * val).all()


@pytest.mark.parametrize("array_size", [2, 2**5, 2**10, 2**15, 2**20])
@pytest.mark.parametrize("src_rank", [0, 1])
@pytest.mark.parametrize("src_gpu_index", [0, 1])
def test_broadcast_different_array_size(
    ray_start_distributed_multigpu_2_nodes_4_gpus, array_size, src_rank, src_gpu_index
):
    world_size = 2
    num_gpu_per_worker = 2
    actors, _ = create_collective_multigpu_workers(world_size)
    ray.get(actors[0].set_buffer.remote([array_size], value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote([array_size], value0=4, value1=5))
    results = ray.get(
        [
            a.do_broadcast_multigpu.remote(
                src_rank=src_rank, src_gpu_index=src_gpu_index
            )
            for a in actors
        ]
    )
    for i in range(world_size):
        for j in range(num_gpu_per_worker):
            val = (src_rank + 1) * 2 + src_gpu_index
            assert (
                results[i][j] == cp.ones((array_size,), dtype=cp.float32) * val
            ).all()


@pytest.mark.parametrize("src_rank", [0, 1])
@pytest.mark.parametrize("src_gpu_index", [0, 1])
def test_broadcast_torch_cupy(
    ray_start_distributed_multigpu_2_nodes_4_gpus, src_rank, src_gpu_index
):
    import torch

    world_size = 2
    num_gpu_per_worker = 2
    actors, _ = create_collective_multigpu_workers(world_size)
    ray.get(actors[0].set_buffer.remote([10], value0=2, value1=3))
    ray.get(
        actors[1].set_buffer.remote(
            [10], value0=4, value1=5, tensor_type0="torch", tensor_type1="torch"
        )
    )
    results = ray.get(
        [
            a.do_broadcast_multigpu.remote(
                src_rank=src_rank, src_gpu_index=src_gpu_index
            )
            for a in actors
        ]
    )
    for i in range(world_size):
        for j in range(num_gpu_per_worker):
            val = (src_rank + 1) * 2 + src_gpu_index
            if i == 0:
                assert (results[i][j] == cp.ones([10], dtype=cp.float32) * val).all()
            else:
                assert (results[i][j] == torch.ones([10]).cuda(j) * val).all()


@pytest.mark.parametrize("src_rank", [3, 4])
@pytest.mark.parametrize("src_gpu_index", [2, 3])
def test_broadcast_invalid_rank(
    ray_start_distributed_multigpu_2_nodes_4_gpus, src_rank, src_gpu_index
):
    world_size = 2
    actors, _ = create_collective_multigpu_workers(world_size)
    with pytest.raises(ValueError):
        _ = ray.get(
            [
                a.do_broadcast_multigpu.remote(
                    src_rank=src_rank, src_gpu_index=src_gpu_index
                )
                for a in actors
            ]
        )
