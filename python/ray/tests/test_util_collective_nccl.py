"""Test util.collective APIs for nccl."""

import os
import sys

import pytest

import ray
import ray.util.collective as col
from ray.util.collective.types import Backend, ReduceOp

if sys.platform != "linux":
    pytest.skip("Skipping, requires Linux.", allow_module_level=True)

USE_GPU = os.environ.get("RAY_PYTEST_USE_GPU") == "1"

# Import GPU libraries after skip logic to avoid ImportError during test collection
try:
    import cupy as cp
    import torch
except ImportError:
    if USE_GPU:
        raise
    # If GPU tests are skipped, these imports are not needed
    cp = None
    torch = None

pytestmark = pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")


@pytest.fixture(scope="module", autouse=True)
def ray_start_single_node_2_gpus():
    # Please start this fixture in a cluster with 2 GPUs.
    address_info = ray.init(num_gpus=2)
    yield address_info
    ray.shutdown()


def cleanup_collective_group(actors, group_name="default"):
    """Clean up actors and collective group after test."""
    try:
        # Destroy the collective group
        ray.get([a.destroy_group.remote(group_name) for a in actors])
    except Exception:
        pass


@pytest.fixture
def defer_cleanup():
    cleanup_tasks = []

    def defer(actors, group_name="default"):
        cleanup_tasks.append((actors, group_name))

    yield defer

    for actors, group_name in reversed(cleanup_tasks):
        cleanup_collective_group(actors, group_name)

    try:
        for actors, _ in reversed(cleanup_tasks):
            for actor in actors:
                ray.kill(actor, no_restart=True)
    except Exception:
        pass


@ray.remote(num_gpus=1)
class Worker:
    def __init__(self):
        self.buffer = None
        self.list_buffer = None

    def init_tensors(self):
        self.buffer = cp.ones((10,), dtype=cp.float32)
        self.list_buffer = [cp.ones((10,), dtype=cp.float32) for _ in range(2)]
        cp.cuda.Stream.null.synchronize()
        return True

    def init_group(self, world_size, rank, backend=Backend.NCCL, group_name="default"):
        col.init_collective_group(world_size, rank, backend, group_name)
        return True

    def set_buffer(self, data):
        self.buffer = data
        return self.buffer

    def set_list_buffer(self, list_of_arrays, copy=False):
        if copy:
            copy_list = []
            for tensor in list_of_arrays:
                if isinstance(tensor, cp.ndarray):
                    copy_list.append(tensor.copy())
                elif torch is not None and isinstance(tensor, torch.Tensor):
                    copy_list.append(tensor.clone().detach())
            self.list_buffer = copy_list
        else:
            self.list_buffer = list_of_arrays
        return self.list_buffer

    def do_allreduce(self, group_name="default", op=ReduceOp.SUM):
        col.allreduce(self.buffer, group_name, op)
        return self.buffer

    def do_reduce(self, group_name="default", dst_rank=0, op=ReduceOp.SUM):
        col.reduce(self.buffer, dst_rank, group_name, op)
        return self.buffer

    def do_broadcast(self, group_name="default", src_rank=0):
        col.broadcast(self.buffer, src_rank, group_name)
        return self.buffer

    def do_allgather(self, group_name="default"):
        col.allgather(self.list_buffer, self.buffer, group_name)
        return self.list_buffer

    def do_reducescatter(self, group_name="default", op=ReduceOp.SUM):
        col.reducescatter(self.buffer, self.list_buffer, group_name, op)
        return self.buffer

    def do_send(self, group_name="default", dst_rank=0):
        col.send(self.buffer, dst_rank, group_name)
        return self.buffer

    def do_recv(self, group_name="default", src_rank=0):
        col.recv(self.buffer, src_rank, group_name)
        return self.buffer

    def destroy_group(self, group_name="default"):
        col.destroy_collective_group(group_name)
        return True


def create_collective_workers(num_workers=2, group_name="default", backend="nccl"):
    actors = [None] * num_workers
    for i in range(num_workers):
        actor = Worker.remote()
        ray.get([actor.init_tensors.remote()])
        actors[i] = actor
    world_size = num_workers
    ranks = list(range(world_size))

    col.create_collective_group(
        actors=actors,
        world_size=world_size,
        ranks=ranks,
        backend=backend,
        group_name=group_name,
        gloo_timeout=30000,
    )

    return actors


def init_tensors_for_gather_scatter(
    actors, array_size=10, dtype=cp.float32, tensor_backend="cupy"
):
    world_size = len(actors)
    for i, a in enumerate(actors):
        if tensor_backend == "cupy":
            t = cp.ones(array_size, dtype=dtype) * (i + 1)
        elif tensor_backend == "torch":
            t = torch.ones(array_size, dtype=torch.float32).cuda() * (i + 1)
        else:
            raise RuntimeError("Unsupported tensor backend.")
        ray.get([a.set_buffer.remote(t)])
    if tensor_backend == "cupy":
        list_buffer = [cp.ones(array_size, dtype=dtype) for _ in range(world_size)]
    elif tensor_backend == "torch":
        list_buffer = [
            torch.ones(array_size, dtype=torch.float32).cuda()
            for _ in range(world_size)
        ]
    else:
        raise RuntimeError("Unsupported tensor backend.")
    ray.get([a.set_list_buffer.remote(list_buffer) for a in actors])


@pytest.mark.parametrize("tensor_backend", ["cupy", "torch"])
@pytest.mark.parametrize("array_size", [[2, 2]])
def test_allgather_different_array_size(array_size, tensor_backend, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    init_tensors_for_gather_scatter(
        actors, array_size=array_size, tensor_backend=tensor_backend
    )
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            if tensor_backend == "cupy":
                assert (
                    results[i][j] == cp.ones(array_size, dtype=cp.float32) * (j + 1)
                ).all()
            else:
                assert (
                    results[i][j]
                    == torch.ones(array_size, dtype=torch.float32).cuda() * (j + 1)
                ).all()


@pytest.mark.parametrize("dtype", [cp.float64])
def test_allgather_different_dtype(dtype, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    init_tensors_for_gather_scatter(actors, dtype=dtype)
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i][j] == cp.ones(10, dtype=dtype) * (j + 1)).all()


@pytest.mark.parametrize("length", [1, 2])
def test_unmatched_tensor_list_length(length, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    list_buffer = [cp.ones(10, dtype=cp.float32) for _ in range(length)]
    ray.wait([a.set_list_buffer.remote(list_buffer) for a in actors])
    if length != world_size:
        with pytest.raises(RuntimeError):
            ray.get([a.do_allgather.remote() for a in actors])
    else:
        ray.get([a.do_allgather.remote() for a in actors])


@pytest.mark.parametrize("shape", [[4, 5]])
def test_unmatched_tensor_shape(shape, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    init_tensors_for_gather_scatter(actors, array_size=10)
    list_buffer = [cp.ones(shape, dtype=cp.float32) for _ in range(world_size)]
    ray.get([a.set_list_buffer.remote(list_buffer) for a in actors])
    if shape != 10:
        with pytest.raises(RuntimeError):
            ray.get([a.do_allgather.remote() for a in actors])
    else:
        ray.get([a.do_allgather.remote() for a in actors])


def test_allgather_torch_cupy(defer_cleanup):
    world_size = 2
    shape = [10, 10]
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)

    # tensor is pytorch, list is cupy
    for i, a in enumerate(actors):
        t = torch.ones(shape, dtype=torch.float32).cuda() * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [cp.ones(shape, dtype=cp.float32) for _ in range(world_size)]
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i][j] == cp.ones(shape, dtype=cp.float32) * (j + 1)).all()

    # tensor is cupy, list is pytorch
    for i, a in enumerate(actors):
        t = cp.ones(shape, dtype=cp.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [
            torch.ones(shape, dtype=torch.float32).cuda() for _ in range(world_size)
        ]
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (
                results[i][j] == torch.ones(shape, dtype=torch.float32).cuda() * (j + 1)
            ).all()

    # some tensors in the list are pytorch, some are cupy
    for i, a in enumerate(actors):
        t = cp.ones(shape, dtype=cp.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = []
        for j in range(world_size):
            if j % 2 == 0:
                list_buffer.append(torch.ones(shape, dtype=torch.float32).cuda())
            else:
                list_buffer.append(cp.ones(shape, dtype=cp.float32))
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_allgather.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            if j % 2 == 0:
                assert (
                    results[i][j]
                    == torch.ones(shape, dtype=torch.float32).cuda() * (j + 1)
                ).all()
            else:
                assert (
                    results[i][j] == cp.ones(shape, dtype=cp.float32) * (j + 1)
                ).all()


@pytest.mark.parametrize("group_name", ["test"])
def test_allreduce_different_name(group_name, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(num_workers=world_size, group_name=group_name)
    defer_cleanup(actors, group_name)
    results = ray.get([a.do_allreduce.remote(group_name) for a in actors])
    assert (results[0] == cp.ones((10,), dtype=cp.float32) * world_size).all()
    assert (results[1] == cp.ones((10,), dtype=cp.float32) * world_size).all()


@pytest.mark.parametrize("array_size", [[2, 2]])
def test_allreduce_different_array_size(array_size, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    ray.wait(
        [a.set_buffer.remote(cp.ones(array_size, dtype=cp.float32)) for a in actors]
    )
    results = ray.get([a.do_allreduce.remote() for a in actors])
    assert (results[0] == cp.ones(array_size, dtype=cp.float32) * world_size).all()
    assert (results[1] == cp.ones(array_size, dtype=cp.float32) * world_size).all()


def test_allreduce_destroy(defer_cleanup, backend="nccl", group_name="default"):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors, group_name)

    results = ray.get([a.do_allreduce.remote() for a in actors])
    assert (results[0] == cp.ones((10,), dtype=cp.float32) * world_size).all()
    assert (results[1] == cp.ones((10,), dtype=cp.float32) * world_size).all()

    # destroy the group and try do work, should fail
    ray.get([a.destroy_group.remote() for a in actors])
    with pytest.raises(RuntimeError):
        results = ray.get([a.do_allreduce.remote() for a in actors])

    # reinit the same group and all reduce
    ray.get(
        [
            actor.init_group.remote(world_size, i, backend, group_name)
            for i, actor in enumerate(actors)
        ]
    )
    results = ray.get([a.do_allreduce.remote() for a in actors])
    assert (results[0] == cp.ones((10,), dtype=cp.float32) * world_size * 2).all()
    assert (results[1] == cp.ones((10,), dtype=cp.float32) * world_size * 2).all()


def test_allreduce_multiple_group(defer_cleanup, backend="nccl", num_groups=5):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    for group_name in range(1, num_groups):
        ray.get(
            [
                actor.init_group.remote(world_size, i, backend, str(group_name))
                for i, actor in enumerate(actors)
            ]
        )
        defer_cleanup(actors, str(group_name))
    for i in range(num_groups):
        group_name = "default" if i == 0 else str(i)
        results = ray.get([a.do_allreduce.remote(group_name) for a in actors])
        assert (
            results[0] == cp.ones((10,), dtype=cp.float32) * (world_size ** (i + 1))
        ).all()


def test_allreduce_different_op(defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)

    # check product
    ray.wait(
        [
            a.set_buffer.remote(cp.ones(10, dtype=cp.float32) * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get([a.do_allreduce.remote(op=ReduceOp.PRODUCT) for a in actors])
    assert (results[0] == cp.ones((10,), dtype=cp.float32) * 6).all()
    assert (results[1] == cp.ones((10,), dtype=cp.float32) * 6).all()

    # check min
    ray.wait(
        [
            a.set_buffer.remote(cp.ones(10, dtype=cp.float32) * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get([a.do_allreduce.remote(op=ReduceOp.MIN) for a in actors])
    assert (results[0] == cp.ones((10,), dtype=cp.float32) * 2).all()
    assert (results[1] == cp.ones((10,), dtype=cp.float32) * 2).all()

    # check max
    ray.wait(
        [
            a.set_buffer.remote(cp.ones(10, dtype=cp.float32) * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get([a.do_allreduce.remote(op=ReduceOp.MAX) for a in actors])
    assert (results[0] == cp.ones((10,), dtype=cp.float32) * 3).all()
    assert (results[1] == cp.ones((10,), dtype=cp.float32) * 3).all()


@pytest.mark.parametrize("dtype", [cp.uint8])
def test_allreduce_different_dtype(dtype, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    ray.wait([a.set_buffer.remote(cp.ones(10, dtype=dtype)) for a in actors])
    results = ray.get([a.do_allreduce.remote() for a in actors])
    assert (results[0] == cp.ones((10,), dtype=dtype) * world_size).all()
    assert (results[1] == cp.ones((10,), dtype=dtype) * world_size).all()


@pytest.mark.parametrize("group_name", ["test"])
@pytest.mark.parametrize("src_rank", [0])
def test_broadcast_different_name(group_name, src_rank, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(num_workers=world_size, group_name=group_name)
    defer_cleanup(actors, group_name)
    ray.wait(
        [
            a.set_buffer.remote(cp.ones((10,), dtype=cp.float32) * (i + 2))
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
        assert (results[i] == cp.ones((10,), dtype=cp.float32) * (src_rank + 2)).all()


@pytest.mark.parametrize("array_size", [[2, 2]])
@pytest.mark.parametrize("src_rank", [0])
def test_broadcast_different_array_size(array_size, src_rank, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    ray.wait(
        [
            a.set_buffer.remote(cp.ones(array_size, dtype=cp.float32) * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get([a.do_broadcast.remote(src_rank=src_rank) for a in actors])
    for i in range(world_size):
        assert (
            results[i] == cp.ones(array_size, dtype=cp.float32) * (src_rank + 2)
        ).all()


@pytest.mark.parametrize("src_rank", [0])
def test_broadcast_torch_cupy(src_rank, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    ray.wait(
        [
            actors[1].set_buffer.remote(
                torch.ones(
                    10,
                ).cuda()
                * world_size
            )
        ]
    )
    results = ray.get([a.do_broadcast.remote(src_rank=src_rank) for a in actors])
    if src_rank == 0:
        assert (results[0] == cp.ones((10,))).all()
        assert (results[1] == torch.ones((10,)).cuda()).all()
    else:
        assert (results[0] == cp.ones((10,)) * world_size).all()
        assert (results[1] == torch.ones((10,)).cuda() * world_size).all()


def test_broadcast_invalid_rank(defer_cleanup, src_rank=3):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    with pytest.raises(ValueError):
        ray.get([a.do_broadcast.remote(src_rank=src_rank) for a in actors])


@pytest.mark.parametrize("group_name", ["test"])
@pytest.mark.parametrize("dst_rank", [0])
def test_reduce_different_name(group_name, dst_rank, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(num_workers=world_size, group_name=group_name)
    defer_cleanup(actors, group_name)
    results = ray.get([a.do_reduce.remote(group_name, dst_rank) for a in actors])
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == cp.ones((10,), dtype=cp.float32) * world_size).all()
        else:
            assert (results[i] == cp.ones((10,), dtype=cp.float32)).all()


@pytest.mark.parametrize("array_size", [2**5])
@pytest.mark.parametrize("dst_rank", [0])
def test_reduce_different_array_size(array_size, dst_rank, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    ray.wait(
        [a.set_buffer.remote(cp.ones(array_size, dtype=cp.float32)) for a in actors]
    )
    results = ray.get([a.do_reduce.remote(dst_rank=dst_rank) for a in actors])
    for i in range(world_size):
        if i == dst_rank:
            assert (
                results[i] == cp.ones((array_size,), dtype=cp.float32) * world_size
            ).all()
        else:
            assert (results[i] == cp.ones((array_size,), dtype=cp.float32)).all()


@pytest.mark.parametrize("dst_rank", [1])
def test_reduce_multiple_group(defer_cleanup, dst_rank, num_groups=5):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    for group_name in range(1, num_groups):
        ray.get(
            [
                actor.init_group.remote(world_size, i, "nccl", str(group_name))
                for i, actor in enumerate(actors)
            ]
        )
        defer_cleanup(actors, str(group_name))

    for i in range(num_groups):
        group_name = "default" if i == 0 else str(i)
        results = ray.get(
            [
                a.do_reduce.remote(dst_rank=dst_rank, group_name=group_name)
                for a in actors
            ]
        )
        for j in range(world_size):
            if j == dst_rank:
                assert (results[j] == cp.ones((10,), dtype=cp.float32) * (i + 2)).all()
            else:
                assert (results[j] == cp.ones((10,), dtype=cp.float32)).all()


@pytest.mark.parametrize("dst_rank", [0])
def test_reduce_different_op(dst_rank, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)

    # check product
    ray.wait(
        [
            a.set_buffer.remote(cp.ones(10, dtype=cp.float32) * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get(
        [a.do_reduce.remote(dst_rank=dst_rank, op=ReduceOp.PRODUCT) for a in actors]
    )
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == cp.ones((10,), dtype=cp.float32) * 6).all()
        else:
            assert (results[i] == cp.ones((10,), dtype=cp.float32) * (i + 2)).all()

    # check min
    ray.wait(
        [
            a.set_buffer.remote(cp.ones(10, dtype=cp.float32) * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get(
        [a.do_reduce.remote(dst_rank=dst_rank, op=ReduceOp.MIN) for a in actors]
    )
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == cp.ones((10,), dtype=cp.float32) * 2).all()
        else:
            assert (results[i] == cp.ones((10,), dtype=cp.float32) * (i + 2)).all()

    # check max
    ray.wait(
        [
            a.set_buffer.remote(cp.ones(10, dtype=cp.float32) * (i + 2))
            for i, a in enumerate(actors)
        ]
    )
    results = ray.get(
        [a.do_reduce.remote(dst_rank=dst_rank, op=ReduceOp.MAX) for a in actors]
    )
    for i in range(world_size):
        if i == dst_rank:
            assert (results[i] == cp.ones((10,), dtype=cp.float32) * 3).all()
        else:
            assert (results[i] == cp.ones((10,), dtype=cp.float32) * (i + 2)).all()


@pytest.mark.parametrize("dst_rank", [0, 1])
def test_reduce_torch_cupy(dst_rank, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    ray.wait(
        [
            actors[1].set_buffer.remote(
                torch.ones(
                    10,
                ).cuda()
            )
        ]
    )
    results = ray.get([a.do_reduce.remote(dst_rank=dst_rank) for a in actors])
    if dst_rank == 0:
        assert (results[0] == cp.ones((10,)) * world_size).all()
        assert (results[1] == torch.ones((10,)).cuda()).all()
    else:
        assert (results[0] == cp.ones((10,))).all()
        assert (results[1] == torch.ones((10,)).cuda() * world_size).all()


def test_reduce_invalid_rank(defer_cleanup, dst_rank=3):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    with pytest.raises(ValueError):
        ray.get([a.do_reduce.remote(dst_rank=dst_rank) for a in actors])


@pytest.mark.parametrize("tensor_backend", ["cupy", "torch"])
@pytest.mark.parametrize("array_size", [[2, 2]])
def test_reducescatter_different_array_size(array_size, tensor_backend, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    init_tensors_for_gather_scatter(
        actors, array_size=array_size, tensor_backend=tensor_backend
    )
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        if tensor_backend == "cupy":
            assert (
                results[i] == cp.ones(array_size, dtype=cp.float32) * world_size
            ).all()
        else:
            assert (
                results[i]
                == torch.ones(array_size, dtype=torch.float32).cuda() * world_size
            ).all()


@pytest.mark.parametrize("dtype", [cp.uint8])
def test_reducescatter_different_dtype(dtype, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    init_tensors_for_gather_scatter(actors, dtype=dtype)
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        for j in range(world_size):
            assert (results[i] == cp.ones(10, dtype=dtype) * world_size).all()


def test_reducescatter_torch_cupy(defer_cleanup):
    world_size = 2
    shape = [10, 10]
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)

    # tensor is pytorch, list is cupy
    for i, a in enumerate(actors):
        t = torch.ones(shape, dtype=torch.float32).cuda() * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [cp.ones(shape, dtype=cp.float32) for _ in range(world_size)]
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        assert (
            results[i] == torch.ones(shape, dtype=torch.float32).cuda() * world_size
        ).all()

    # tensor is cupy, list is pytorch
    for i, a in enumerate(actors):
        t = cp.ones(shape, dtype=cp.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = [
            torch.ones(shape, dtype=torch.float32).cuda() for _ in range(world_size)
        ]
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        assert (results[i] == cp.ones(shape, dtype=cp.float32) * world_size).all()

    # some tensors in the list are pytorch, some are cupy
    for i, a in enumerate(actors):
        if i % 2 == 0:
            t = torch.ones(shape, dtype=torch.float32).cuda() * (i + 1)
        else:
            t = cp.ones(shape, dtype=cp.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = []
        for j in range(world_size):
            if j % 2 == 0:
                list_buffer.append(torch.ones(shape, dtype=torch.float32).cuda())
            else:
                list_buffer.append(cp.ones(shape, dtype=cp.float32))
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        if i % 2 == 0:
            assert (
                results[i] == torch.ones(shape, dtype=torch.float32).cuda() * world_size
            ).all()
        else:
            assert (results[i] == cp.ones(shape, dtype=cp.float32) * world_size).all()

    # mixed case
    for i, a in enumerate(actors):
        if i % 2 == 0:
            t = torch.ones(shape, dtype=torch.float32).cuda() * (i + 1)
        else:
            t = cp.ones(shape, dtype=cp.float32) * (i + 1)
        ray.wait([a.set_buffer.remote(t)])
        list_buffer = []
        for j in range(world_size):
            if j % 2 == 0:
                list_buffer.append(cp.ones(shape, dtype=cp.float32))
            else:
                list_buffer.append(torch.ones(shape, dtype=torch.float32).cuda())
        ray.wait([a.set_list_buffer.remote(list_buffer)])
    results = ray.get([a.do_reducescatter.remote() for a in actors])
    for i in range(world_size):
        if i % 2 == 0:
            assert (
                results[i] == torch.ones(shape, dtype=torch.float32).cuda() * world_size
            ).all()
        else:
            assert (results[i] == cp.ones(shape, dtype=cp.float32) * world_size).all()


@pytest.mark.parametrize("group_name", ["test"])
@pytest.mark.parametrize("dst_rank", [0])
@pytest.mark.parametrize("array_size", [[2, 2]])
def test_sendrecv_different_name(group_name, array_size, dst_rank, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(num_workers=world_size, group_name=group_name)
    defer_cleanup(actors, group_name)
    ray.wait(
        [
            a.set_buffer.remote(cp.ones(array_size, dtype=cp.float32) * (i + 1))
            for i, a in enumerate(actors)
        ]
    )
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
        assert (
            results[i] == cp.ones(array_size, dtype=cp.float32) * (src_rank + 1)
        ).all()


@pytest.mark.parametrize("dst_rank", [1])
def test_sendrecv_torch_cupy(dst_rank, defer_cleanup):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    ray.wait(
        [
            actors[1].set_buffer.remote(
                torch.ones(
                    10,
                ).cuda()
                * 2
            )
        ]
    )
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
        assert (results[0] == cp.ones((10,)) * 2).all()
    else:
        assert (results[0] == cp.ones((10,))).all()
        assert (results[1] == torch.ones((10,)).cuda()).all()


def test_sendrecv_invalid_rank(defer_cleanup, dst_rank=3):
    world_size = 2
    actors = create_collective_workers(world_size)
    defer_cleanup(actors)
    with pytest.raises(ValueError):
        ray.get([a.do_send.remote(dst_rank=dst_rank) for a in actors])


def test_create_collective_group_driver_only(defer_cleanup):
    world_size = 2
    group_name = "test"
    actors = create_collective_workers(num_workers=world_size, group_name=group_name)
    defer_cleanup(actors, group_name)
    results = ray.get([a.do_allreduce.remote(group_name=group_name) for a in actors])
    for r in results:
        assert (r == cp.ones((10,), dtype=cp.float32) * world_size).all()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
