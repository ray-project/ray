import sys

import pytest
import torch

import ray
import ray.experimental.collective

SHAPE = (2, 2)
DTYPE = torch.float16


@ray.remote
class Actor:
    def __init__(self, shape, dtype):
        self.tensor = torch.zeros(shape, dtype=dtype)

    def make_tensor(self, shape, dtype):
        self.tensor = torch.randn(shape, dtype=dtype)

    def get_tensor(self):
        return self.tensor


@pytest.fixture
def collective_actors():
    world_size = 3
    actors = [Actor.remote(SHAPE, DTYPE) for _ in range(world_size)]

    group = ray.experimental.collective.create_collective_group(
        actors, backend="torch_gloo"
    )
    return group.name, actors


def test_api_basic(ray_start_regular_shared):
    world_size = 3
    actors = [Actor.remote(SHAPE, DTYPE) for _ in range(world_size)]

    # Check no groups on start up.
    for actor in actors:
        groups = ray.experimental.collective.get_collective_groups([actor])
        assert groups == []

    groups = ray.experimental.collective.get_collective_groups(actors)
    assert groups == []

    # Check that the collective group is created with the correct actors and
    # ranks.
    group = ray.experimental.collective.create_collective_group(
        actors, backend="torch_gloo", name="test"
    )
    assert group.name == "test"
    for i, actor in enumerate(actors):
        assert group.get_rank(actor) == i

    # Check that we can look up the created collective by actor handle(s).
    for actor in actors:
        groups = ray.experimental.collective.get_collective_groups([actor])
        assert groups == [group]

    groups = ray.experimental.collective.get_collective_groups(actors)
    assert groups == [group]

    # Check that the group is destroyed.
    ray.experimental.collective.destroy_collective_group(group)

    for actor in actors:
        groups = ray.experimental.collective.get_collective_groups([actor])
        assert groups == []

    groups = ray.experimental.collective.get_collective_groups(actors)
    assert groups == []

    # Check that we can recreate the group with the same name and actors.
    ray.experimental.collective.create_collective_group(
        actors, backend="torch_gloo", name="test"
    )


def test_api_exceptions(ray_start_regular_shared):
    world_size = 3
    actors = [Actor.remote(SHAPE, DTYPE) for _ in range(world_size)]

    with pytest.raises(ValueError, match="All actors must be unique"):
        ray.experimental.collective.create_collective_group(
            actors + [actors[0]], "torch_gloo"
        )

    ray.experimental.collective.create_collective_group(actors, backend="torch_gloo")

    # Check that we cannot create another group using the same actors.
    with pytest.raises(RuntimeError, match="already in group"):
        ray.experimental.collective.create_collective_group(
            actors, backend="torch_gloo"
        )
    with pytest.raises(RuntimeError, match="already in group"):
        ray.experimental.collective.create_collective_group(
            actors[:2], backend="torch_gloo"
        )
    with pytest.raises(RuntimeError, match="already in group"):
        ray.experimental.collective.create_collective_group(
            actors[1:], backend="torch_gloo"
        )


def test_allreduce(ray_start_regular_shared, collective_actors):
    group_name, actors = collective_actors

    [actor.make_tensor.remote(SHAPE, DTYPE) for actor in actors]
    tensors = ray.get([actor.get_tensor.remote() for actor in actors])
    expected_sum = sum(tensors)

    def do_allreduce(self, group_name):
        ray.util.collective.allreduce(self.tensor, group_name=group_name)

    ray.get([actor.__ray_call__.remote(do_allreduce, group_name) for actor in actors])
    tensors = ray.get([actor.get_tensor.remote() for actor in actors])
    for tensor in tensors:
        assert torch.allclose(tensor, expected_sum, atol=1e-2)


def test_barrier(ray_start_regular_shared, collective_actors):
    group_name, actors = collective_actors

    def do_barrier(self, group_name):
        ray.util.collective.barrier(group_name=group_name)

    barriers = []
    for actor in actors:
        if barriers:
            with pytest.raises(ray.exceptions.GetTimeoutError):
                ray.get(barriers, timeout=0.1)
        barriers.append(actor.__ray_call__.remote(do_barrier, group_name))
    ray.get(barriers)


def test_allgather(ray_start_regular_shared, collective_actors):
    group_name, actors = collective_actors

    [actor.make_tensor.remote(SHAPE, DTYPE) for actor in actors]
    tensors = ray.get([actor.get_tensor.remote() for actor in actors])

    def do_allgather(self, world_size, group_name):
        tensor_list = [torch.zeros(SHAPE, dtype=DTYPE) for _ in range(world_size)]
        ray.util.collective.allgather(tensor_list, self.tensor, group_name=group_name)
        return tensor_list

    all_tensor_lists = ray.get(
        [
            actor.__ray_call__.remote(do_allgather, len(actors), group_name)
            for actor in actors
        ]
    )
    for tensor_list in all_tensor_lists:
        for tensor, expected_tensor in zip(tensors, tensor_list):
            assert torch.allclose(tensor, expected_tensor)


def test_broadcast(ray_start_regular_shared, collective_actors):
    group_name, actors = collective_actors

    actors[0].make_tensor.remote(SHAPE, DTYPE)
    expected_tensor = ray.get(actors[0].get_tensor.remote())

    def do_broadcast(self, src_rank, group_name):
        ray.util.collective.broadcast(self.tensor, src_rank, group_name=group_name)

    [actor.__ray_call__.remote(do_broadcast, 0, group_name) for actor in actors]
    tensors = ray.get([actor.get_tensor.remote() for actor in actors])
    for tensor in tensors:
        assert torch.allclose(tensor, expected_tensor)


def test_reduce(ray_start_regular_shared, collective_actors):
    group_name, actors = collective_actors

    [actor.make_tensor.remote(SHAPE, DTYPE) for actor in actors]
    tensors = ray.get([actor.get_tensor.remote() for actor in actors])
    expected_sum = sum(tensors)

    def do_reduce(self, dst_rank, group_name):
        ray.util.collective.reduce(self.tensor, dst_rank, group_name)

    dst_rank = 0
    ray.get(
        [actor.__ray_call__.remote(do_reduce, dst_rank, group_name) for actor in actors]
    )
    tensor = ray.get(actors[dst_rank].get_tensor.remote())
    assert torch.allclose(tensor, expected_sum, atol=1e-2)


def test_reducescatter(ray_start_regular_shared, collective_actors):
    group_name, actors = collective_actors

    [actor.make_tensor.remote((len(actors), *SHAPE), DTYPE) for actor in actors]
    tensors = ray.get([actor.get_tensor.remote() for actor in actors])
    expected_sum = sum(tensors)
    expected_tensors = list(expected_sum)

    def do_reducescatter(self, world_size, group_name):
        tensor = torch.zeros(SHAPE, dtype=DTYPE)
        tensor_list = list(self.tensor)
        ray.util.collective.reducescatter(tensor, tensor_list, group_name)
        return tensor

    tensors = ray.get(
        [
            actor.__ray_call__.remote(do_reducescatter, len(actors), group_name)
            for actor in actors
        ]
    )
    for tensor, expected in zip(tensors, expected_tensors):
        assert torch.allclose(tensor, expected, atol=1e-2)


def test_send_recv(ray_start_regular_shared, collective_actors):
    group_name, actors = collective_actors

    def do_send(self, group_name, dst_rank):
        ray.util.collective.send(self.tensor, dst_rank, group_name=group_name)

    def do_recv(self, group_name, src_rank):
        ray.util.collective.recv(self.tensor, src_rank, group_name=group_name)

    for ranks in [(0, 1), (1, 2), (2, 0)]:
        src_rank, dst_rank = ranks
        src, dst = actors[src_rank], actors[dst_rank]
        src.make_tensor.remote(SHAPE, DTYPE)
        tensor = ray.get(src.get_tensor.remote())
        ray.get(
            [
                src.__ray_call__.remote(do_send, group_name, dst_rank),
                dst.__ray_call__.remote(do_recv, group_name, src_rank),
            ]
        )

        assert torch.allclose(tensor, ray.get(src.get_tensor.remote()))
        assert torch.allclose(tensor, ray.get(dst.get_tensor.remote()))


def test_send_recv_exceptions(ray_start_regular_shared, collective_actors):
    group_name, actors = collective_actors

    def do_send(self, group_name, dst_rank):
        ray.util.collective.send(self.tensor, dst_rank, group_name=group_name)

    def do_recv(self, group_name, src_rank):
        ray.util.collective.recv(self.tensor, src_rank, group_name=group_name)

    # Actors cannot send to/recv from themselves.
    for rank in range(len(actors)):
        with pytest.raises(RuntimeError):
            ray.get(actors[rank].__ray_call__.remote(do_send, group_name, rank))
        with pytest.raises(RuntimeError):
            ray.get(actors[rank].__ray_call__.remote(do_recv, group_name, rank))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
