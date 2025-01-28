# coding: utf-8
import logging
import os
import sys
import ray.experimental.collective as collective

import pytest
from ray.experimental.collective.conftest import (
    AbstractNcclGroup,
    CPUTorchTensorWorker,
    check_nccl_group_init,
    check_nccl_group_teardown,
)
from ray.dag import InputNode, MultiOutputNode
from ray.tests.conftest import *  # noqa

logger = logging.getLogger(__name__)

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_all_reduce_duplicate_actors(ray_start_regular):
    """
    Test an error is thrown when two input nodes from the same actor bind to
    an all-reduce.
    """
    actor_cls = CPUTorchTensorWorker.options()
    worker = actor_cls.remote()

    with InputNode() as inp:
        computes = [worker.return_tensor.bind(inp) for _ in range(2)]
        with pytest.raises(
            ValueError,
            match="Expected unique actor handles for a collective operation",
        ):
            collective.allreduce.bind(computes)

    with InputNode() as inp:
        compute = worker.return_tensor.bind(inp)
        computes = [compute for _ in range(2)]
        with pytest.raises(
            ValueError,
            match="Expected unique input nodes for a collective operation",
        ):
            collective.allreduce.bind(computes)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_all_reduce_custom_comm_wrong_actors(ray_start_regular):
    """
    Test an error is thrown when an all-reduce binds to a custom NCCL group and
    a wrong set of actors.
    """
    actor_cls = CPUTorchTensorWorker.options()

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    nccl_group = AbstractNcclGroup([workers[0]])
    with InputNode() as inp:
        computes = [worker.return_tensor.bind(inp) for worker in workers]
        with pytest.raises(
            ValueError,
            match="Expected actor handles to match the custom NCCL group",
        ):
            collective.allreduce.bind(computes, transport=nccl_group)


@pytest.mark.parametrize(
    "ray_start_regular", [{"num_cpus": 4, "num_gpus": 4}], indirect=True
)
def test_comm_all_reduces(ray_start_regular, monkeypatch):
    """
    Test different communicators are used for different all-reduce calls of
    different sets of actors.
    """
    actor_cls = CPUTorchTensorWorker.options(num_cpus=0, num_gpus=1)

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    with InputNode() as inp:
        computes = [worker.return_tensor.bind(inp) for worker in workers]
        # There are two all-reduces, each on one actor.
        collectives = [collective.allreduce.bind([compute]) for compute in computes]
        # collective[0] is the only CollectiveOutputNode for each all-reduce.
        dag = MultiOutputNode([collective[0] for collective in collectives])

    compiled_dag, mock_nccl_group_set = check_nccl_group_init(
        monkeypatch,
        dag,
        {
            (frozenset(workers), None),
        },
        (frozenset(workers), None),
    )

    check_nccl_group_teardown(monkeypatch, compiled_dag, mock_nccl_group_set)


@pytest.mark.parametrize(
    "ray_start_regular", [{"num_cpus": 4, "num_gpus": 4}], indirect=True
)
def test_comm_deduplicate_all_reduces(ray_start_regular, monkeypatch):
    """
    Test communicators are deduplicated when all-reduces are called on the same
    group of actors more than once.
    """
    actor_cls = CPUTorchTensorWorker.options(num_cpus=0, num_gpus=1)

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    with InputNode() as inp:
        tensors = [worker.return_tensor.bind(inp) for worker in workers]
        collectives = collective.allreduce.bind(tensors)
        collectives = collective.allreduce.bind(collectives)
        dag = MultiOutputNode(collectives)

    compiled_dag, mock_nccl_group_set = check_nccl_group_init(
        monkeypatch, dag, {(frozenset(workers), None)}, (frozenset(workers), None)
    )

    check_nccl_group_teardown(monkeypatch, compiled_dag, mock_nccl_group_set)


@pytest.mark.parametrize(
    "ray_start_regular", [{"num_cpus": 4, "num_gpus": 4}], indirect=True
)
def test_comm_deduplicate_p2p_and_collective(ray_start_regular, monkeypatch):
    """
    Test communicators are deduplicated when the collective and the P2P are on
    the same set of actors.
    """
    actor_cls = CPUTorchTensorWorker.options(num_cpus=0, num_gpus=1)

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    with InputNode() as inp:
        computes = [worker.return_tensor.bind(inp) for worker in workers]
        collectives = collective.allreduce.bind(computes)
        recvs = [
            # Each of the 2 workers receives from the other.
            workers[0].recv.bind(
                collectives[1].with_tensor_transport(transport="nccl")
            ),
            workers[1].recv.bind(
                collectives[0].with_tensor_transport(transport="nccl")
            ),
        ]
        dag = MultiOutputNode(recvs)

    compiled_dag, mock_nccl_group_set = check_nccl_group_init(
        monkeypatch,
        dag,
        {(frozenset(workers), None)},
        (frozenset(workers), None),
    )

    check_nccl_group_teardown(monkeypatch, compiled_dag, mock_nccl_group_set)

    with InputNode() as inp:
        computes = [worker.return_tensor.bind(inp) for worker in workers]
        collectives = collective.allreduce.bind(computes)
        # Sender is workers[0] and receiver is workers[1].
        dag = workers[1].recv.bind(
            collectives[0].with_tensor_transport(transport="nccl")
        )
        dag = MultiOutputNode([dag, collectives[1]])

    compiled_dag, mock_nccl_group_set = check_nccl_group_init(
        monkeypatch,
        dag,
        {(frozenset(workers), None)},
        (frozenset(workers), None),
    )

    check_nccl_group_teardown(monkeypatch, compiled_dag, mock_nccl_group_set)


@pytest.mark.parametrize(
    "ray_start_regular", [{"num_cpus": 4, "num_gpus": 4}], indirect=True
)
def test_custom_comm_deduplicate(ray_start_regular, monkeypatch):
    """
    Test a custom GPU communicator is reused when possible.
    """
    actor_cls = CPUTorchTensorWorker.options(num_cpus=0, num_gpus=1)

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    comm = AbstractNcclGroup(workers)
    with InputNode() as inp:
        computes = [worker.return_tensor.bind(inp) for worker in workers]
        collectives = collective.allreduce.bind(computes, transport=comm)
        collectives = collective.allreduce.bind(collectives)
        dag = workers[0].recv.bind(
            collectives[1].with_tensor_transport(transport="nccl")
        )
        dag = MultiOutputNode([dag, collectives[0]])

    compiled_dag, mock_nccl_group_set = check_nccl_group_init(
        monkeypatch,
        dag,
        {
            (frozenset(workers), comm),
            (frozenset(workers), None),
        },
        (frozenset(workers), None),
    )

    check_nccl_group_teardown(monkeypatch, compiled_dag, mock_nccl_group_set)

    comm = AbstractNcclGroup(workers)
    with InputNode() as inp:
        computes = [worker.return_tensor.bind(inp) for worker in workers]
        collectives = collective.allreduce.bind(computes)
        collectives = collective.allreduce.bind(collectives)
        dag = workers[0].recv.bind(collectives[1].with_tensor_transport(transport=comm))
        dag = MultiOutputNode([dag, collectives[0]])

    compiled_dag, mock_nccl_group_set = check_nccl_group_init(
        monkeypatch,
        dag,
        {
            (frozenset(workers), comm),
            (frozenset(workers), None),
        },
        (frozenset(workers), None),
    )

    check_nccl_group_teardown(monkeypatch, compiled_dag, mock_nccl_group_set)


@pytest.mark.parametrize(
    "ray_start_regular", [{"num_cpus": 4, "num_gpus": 4}], indirect=True
)
def test_custom_comm_init_teardown(ray_start_regular, monkeypatch):
    """
    Test custom NCCL groups are properly initialized and destroyed.
    1. Test when multiple type hints have the same `transport=custom_nccl_group`,
    the `custom_nccl_group` is initialized only once.
    2. Test all initialized NCCL groups are destroyed during teardown.
    """
    actor_cls = CPUTorchTensorWorker.options(num_cpus=0, num_gpus=1)

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    comm = AbstractNcclGroup(workers)

    with InputNode() as inp:
        tensors = [worker.return_tensor.bind(inp) for worker in workers]
        allreduce = collective.allreduce.bind(tensors, transport=comm)
        dag = workers[0].recv.bind(allreduce[1].with_tensor_transport(transport=comm))
        dag = MultiOutputNode([dag, allreduce[0]])

    compiled_dag, mock_nccl_group_set = check_nccl_group_init(
        monkeypatch,
        dag,
        {(frozenset(workers), comm)},
    )

    check_nccl_group_teardown(monkeypatch, compiled_dag, mock_nccl_group_set)

    comm_1 = AbstractNcclGroup(workers)
    comm_2 = AbstractNcclGroup(workers)
    comm_3 = AbstractNcclGroup(workers)

    with InputNode() as inp:
        tensors = [worker.return_tensor.bind(inp) for worker in workers]
        allreduce1 = collective.allreduce.bind(tensors, transport=comm_1)
        allreduce2 = collective.allreduce.bind(allreduce1, transport=comm_2)
        dag = workers[0].recv.bind(
            allreduce2[1].with_tensor_transport(transport=comm_3)
        )
        dag = MultiOutputNode([dag, allreduce2[0]])

    compiled_dag, mock_nccl_group_set = check_nccl_group_init(
        monkeypatch,
        dag,
        {
            (frozenset(workers), comm_1),
            (frozenset(workers), comm_2),
            (frozenset(workers), comm_3),
        },
    )

    check_nccl_group_teardown(monkeypatch, compiled_dag, mock_nccl_group_set)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
