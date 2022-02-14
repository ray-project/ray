"""Test the send/recv API."""
import numpy as np
import pytest
import ray

from ray.util.collective.types import Backend
from ray.util.collective.tests.cpu_util import create_collective_workers


@pytest.mark.parametrize("backend", [Backend.GLOO])
@pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("dst_rank", [0, 1, 3, 6])
@pytest.mark.parametrize("src_rank", [0, 2, 4, 7])
@pytest.mark.parametrize(
    "array_size", [2 ** 10, 2 ** 15, 2 ** 20, [2, 2], [5, 9, 10, 85]]
)
def test_sendrecv(
    ray_start_distributed_2_nodes, group_name, array_size, src_rank, dst_rank, backend
):
    if src_rank == dst_rank:
        return
    world_size = 8
    actors, _ = create_collective_workers(
        num_workers=world_size, group_name=group_name, backend=backend
    )
    ray.get(
        [
            a.set_buffer.remote(np.ones(array_size, dtype=np.float32) * (i + 1))
            for i, a in enumerate(actors)
        ]
    )
    refs = []
    for i in range(world_size):
        refs.append(actors[i].get_buffer.remote())
    refs[src_rank] = actors[src_rank].do_send.remote(group_name, dst_rank)
    refs[dst_rank] = actors[dst_rank].do_recv.remote(group_name, src_rank)
    results = ray.get(refs)
    assert (
        results[src_rank] == np.ones(array_size, dtype=np.float32) * (src_rank + 1)
    ).all()
    assert (
        results[dst_rank] == np.ones(array_size, dtype=np.float32) * (src_rank + 1)
    ).all()
    ray.get([a.destroy_group.remote(group_name) for a in actors])


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
