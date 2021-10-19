"""Test the send/recv API."""
import cupy as cp
import pytest
import ray

from ray.util.collective.tests.util import create_collective_multigpu_workers


# @pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("dst_rank", [0, 1])
@pytest.mark.parametrize("src_rank", [0, 1])
@pytest.mark.parametrize("dst_gpu_index", [0, 1])
@pytest.mark.parametrize("src_gpu_index", [0, 1])
@pytest.mark.parametrize("array_size",
                         [2**10, 2**15, 2**20, [2, 2], [5, 9, 10, 85]])
def test_sendrecv(ray_start_distributed_multigpu_2_nodes_4_gpus, array_size,
                  src_rank, dst_rank, src_gpu_index, dst_gpu_index):
    if src_rank == dst_rank:
        return
    world_size = 2
    actors, _ = create_collective_multigpu_workers(num_workers=world_size)

    ray.get(actors[0].set_buffer.remote(array_size, value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote(array_size, value0=4, value1=5))

    refs = []
    for i in range(world_size):
        refs.append(actors[i].get_buffer.remote())
    refs[src_rank][src_gpu_index] = actors[src_rank].do_send_multigpu.remote(
        dst_rank=dst_rank,
        dst_gpu_index=dst_gpu_index,
        src_gpu_index=src_gpu_index)
    refs[dst_rank][dst_gpu_index] = actors[dst_rank].do_recv_multigpu.remote(
        src_rank=src_rank,
        src_gpu_index=src_gpu_index,
        dst_gpu_index=dst_gpu_index)
    results = []
    results_flattend = ray.get(refs[0] + refs[1])
    results.append([results_flattend[0], results_flattend[1]])
    results.append([results_flattend[2], results_flattend[3]])
    assert (results[src_rank][src_gpu_index] == cp.ones(
        array_size, dtype=cp.float32) * (
            (src_rank + 1) * 2 + src_gpu_index)).all()
    assert (results[dst_rank][dst_gpu_index] == cp.ones(
        array_size, dtype=cp.float32) * (
            (src_rank + 1) * 2 + src_gpu_index)).all()
    ray.get([a.destroy_group.remote() for a in actors])
