# coding: utf-8
import logging
import os
import sys
import uuid
import copy
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

import pytest
import ray
import ray.cluster_utils
import ray.experimental.collective as collective
import torch
from ray.dag import InputNode, MultiOutputNode
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.experimental.channel.common import ChannelContext
from ray.experimental.channel.gpu_communicator import (
    GPUCommunicator,
    TorchTensorAllocator,
)
from ray.tests.conftest import *  # noqa
from ray.util.collective.types import ReduceOp

logger = logging.getLogger(__name__)

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)


@ray.remote
class CPUTorchTensorWorker:
    def __init__(self):
        self.device = "cpu"

    def return_tensor(self, size: int) -> torch.Tensor:
        return torch.ones(size, device=self.device)

    def recv(self, tensor: torch.Tensor) -> Tuple[int, int]:
        assert tensor.device == self.device
        return tensor.shape, tensor[0]


def mock_do_init_nccl_group(
    self,
    group_id: str,
    rank: int,
    actors: List[ray.actor.ActorHandle],
    custom_nccl_group: Optional[GPUCommunicator],
) -> None:
    ctx = ChannelContext.get_current()
    if custom_nccl_group is None:
        nccl_group = AbstractNcclGroup(actors)
        nccl_group.initialize(rank)
        ctx.nccl_groups[group_id] = nccl_group
    else:
        custom_nccl_group.initialize(rank)
        ctx.nccl_groups[group_id] = custom_nccl_group


def mock_do_destroy_nccl_group(self, group_id: str) -> None:
    ctx = ChannelContext.get_current()
    if group_id not in ctx.nccl_groups:
        return
    ctx.nccl_groups[group_id].destroy()
    del ctx.nccl_groups[group_id]


class AbstractNcclGroup(GPUCommunicator):
    """
    A dummy NCCL group for testing.
    """

    def __init__(self, actor_handles: List[ray.actor.ActorHandle]):
        self._actor_handles = actor_handles
        self._rank = None

    def initialize(self, rank: int) -> None:
        self._rank = rank

    def get_rank(self, actor: ray.actor.ActorHandle) -> int:
        return self._actor_handles.index(actor)

    def get_world_size(self) -> int:
        return len(self._actor_handles)

    def get_self_rank(self) -> Optional[int]:
        return self._rank

    def get_actor_handles(self) -> List["ray.actor.ActorHandle"]:
        return self._actor_handles

    def send(self, value: "torch.Tensor", peer_rank: int) -> None:
        raise NotImplementedError

    def recv(
        self,
        shape: Tuple[int],
        dtype: "torch.dtype",
        peer_rank: int,
        allocator: Optional[TorchTensorAllocator] = None,
    ) -> "torch.Tensor":
        raise NotImplementedError

    def allreduce(
        self,
        send_buf: "torch.Tensor",
        recv_buf: "torch.Tensor",
        op: ReduceOp = ReduceOp.SUM,
    ) -> None:
        raise NotImplementedError

    def destroy(self) -> None:
        pass


class MockNcclGroupSet:
    def __init__(self):
        # Represents a mapping from a NCCL group ID to a set of actors and a custom
        # NCCL group.
        self.ids_to_actors_and_custom_comms: Dict[
            str, Tuple[FrozenSet["ray.actor.ActorHandle"], Optional[GPUCommunicator]]
        ] = {}

    def __call__(
        self,
        actors: List["ray.actor.ActorHandle"],
        custom_nccl_group: Optional[GPUCommunicator] = None,
    ) -> str:
        group_id = str(uuid.uuid4())
        self.ids_to_actors_and_custom_comms[group_id] = (
            frozenset(actors),
            custom_nccl_group,
        )

        if custom_nccl_group is None:
            ranks = list(range(len(actors)))
        else:
            ranks = [custom_nccl_group.get_rank(actor) for actor in actors]
        init_tasks = [
            actor.__ray_call__.remote(
                mock_do_init_nccl_group,
                group_id,
                rank,
                actors,
                custom_nccl_group,
            )
            for rank, actor in zip(ranks, actors)
        ]
        ray.get(init_tasks, timeout=30)

        ctx = ChannelContext.get_current()
        if custom_nccl_group is not None:
            ctx.nccl_groups[group_id] = custom_nccl_group
        else:
            ctx.nccl_groups[group_id] = AbstractNcclGroup(actors)

        return group_id

    def mock_destroy_nccl_group(self, group_id: str) -> None:
        ctx = ChannelContext.get_current()
        if group_id not in ctx.nccl_groups:
            return

        actors, _ = self.ids_to_actors_and_custom_comms[group_id]
        destroy_tasks = [
            actor.__ray_call__.remote(
                mock_do_destroy_nccl_group,
                group_id,
            )
            for actor in actors
        ]
        ray.wait(destroy_tasks, timeout=30)

        if group_id in self.ids_to_actors_and_custom_comms:
            del self.ids_to_actors_and_custom_comms[group_id]
        ctx.nccl_groups[group_id].destroy()
        del ctx.nccl_groups[group_id]

    def check_init(
        self,
        compiled_dag: "ray.dag.CompiledDAG",
        actors_and_custom_comms: Set[
            Tuple[FrozenSet["ray.actor.ActorHandle"], Optional[GPUCommunicator]]
        ],
        p2p_actors_and_custom_comm: Optional[
            Tuple[FrozenSet["ray.actor.ActorHandle"], Optional[GPUCommunicator]]
        ],
    ) -> None:
        assert len(self.ids_to_actors_and_custom_comms) == len(actors_and_custom_comms)
        assert (
            set(self.ids_to_actors_and_custom_comms.values()) == actors_and_custom_comms
        )

        nccl_group_id_p2p = compiled_dag.nccl_group_id_p2p
        if p2p_actors_and_custom_comm is None:
            assert nccl_group_id_p2p is None
        else:
            assert nccl_group_id_p2p
            assert (
                self.ids_to_actors_and_custom_comms[nccl_group_id_p2p]
                == p2p_actors_and_custom_comm
            )

    def check_teardown(self, nccl_group_ids: List[str]) -> None:
        ctx = ChannelContext.get_current()
        for nccl_group_id in nccl_group_ids:
            assert nccl_group_id not in self.ids_to_actors_and_custom_comms
            assert nccl_group_id not in ctx.nccl_groups


def check_nccl_group_init(
    monkeypatch,
    dag: "ray.dag.DAGNode",
    actors_and_custom_comms: Set[
        Tuple[FrozenSet["ray.actor.ActorHandle"], Optional[GPUCommunicator]]
    ],
    p2p_actors_and_custom_comm: Optional[
        Tuple[FrozenSet["ray.actor.ActorHandle"], Optional[GPUCommunicator]]
    ] = None,
) -> "ray.dag.CompiledDAG":
    mock_nccl_group_set = MockNcclGroupSet()
    monkeypatch.setattr(
        "ray.dag.compiled_dag_node._init_nccl_group",
        mock_nccl_group_set,
    )
    monkeypatch.setattr(
        "ray.dag.collective_node._init_nccl_group",
        mock_nccl_group_set,
    )

    compiled_dag = dag.experimental_compile()
    mock_nccl_group_set.check_init(
        compiled_dag,
        actors_and_custom_comms,
        p2p_actors_and_custom_comm,
    )

    return compiled_dag, mock_nccl_group_set


def check_nccl_group_teardown(
    monkeypatch,
    compiled_dag: "ray.dag.CompiledDAG",
    mock_nccl_group_set: MockNcclGroupSet,
):
    monkeypatch.setattr(
        "ray.dag.compiled_dag_node._destroy_nccl_group",
        mock_nccl_group_set.mock_destroy_nccl_group,
    )

    nccl_group_ids = copy.deepcopy(compiled_dag.nccl_group_ids)
    compiled_dag.teardown()
    mock_nccl_group_set.check_teardown(nccl_group_ids)


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
            (frozenset([workers[0]]), None),
            (frozenset([workers[1]]), None),
        },
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
        monkeypatch,
        dag,
        {(frozenset(workers), None)},
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
                collectives[1].with_type_hint(TorchTensorType(transport="nccl"))
            ),
            workers[1].recv.bind(
                collectives[0].with_type_hint(TorchTensorType(transport="nccl"))
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
            collectives[0].with_type_hint(TorchTensorType(transport="nccl"))
        )

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
            collectives[1].with_type_hint(TorchTensorType(transport="nccl"))
        )

    compiled_dag, mock_nccl_group_set = check_nccl_group_init(
        monkeypatch,
        dag,
        {(frozenset(workers), comm)},
        (frozenset(workers), comm),
    )

    check_nccl_group_teardown(monkeypatch, compiled_dag, mock_nccl_group_set)

    comm = AbstractNcclGroup(workers)
    with InputNode() as inp:
        computes = [worker.return_tensor.bind(inp) for worker in workers]
        collectives = collective.allreduce.bind(computes)
        collectives = collective.allreduce.bind(collectives)
        dag = workers[0].recv.bind(
            collectives[1].with_type_hint(TorchTensorType(transport=comm))
        )

    compiled_dag, mock_nccl_group_set = check_nccl_group_init(
        monkeypatch,
        dag,
        {(frozenset(workers), comm)},
        (frozenset(workers), comm),
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
        dag = workers[0].recv.bind(
            allreduce[1].with_type_hint(TorchTensorType(transport=comm))
        )

    compiled_dag, mock_nccl_group_set = check_nccl_group_init(
        monkeypatch,
        dag,
        {(frozenset(workers), comm)},
        (frozenset(workers), comm),
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
            allreduce2[1].with_type_hint(TorchTensorType(transport=comm_3))
        )

    compiled_dag, mock_nccl_group_set = check_nccl_group_init(
        monkeypatch,
        dag,
        {
            (frozenset(workers), comm_1),
            (frozenset(workers), comm_2),
            (frozenset(workers), comm_3),
        },
        (frozenset(workers), comm_3),
    )

    check_nccl_group_teardown(monkeypatch, compiled_dag, mock_nccl_group_set)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
