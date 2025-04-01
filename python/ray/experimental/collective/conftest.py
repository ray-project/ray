import uuid
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

import torch

import ray
from ray.experimental.channel.common import ChannelContext
from ray.experimental.channel.communicator import (
    Communicator,
    ReduceOp,
    TorchTensorAllocator,
)


class AbstractNcclGroup(Communicator):
    """
    A dummy NCCL group for testing.
    """

    import cupy as cp

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

    @property
    def recv_stream(self) -> Optional["cp.cuda.ExternalStream"]:
        return None

    @property
    def send_stream(self) -> Optional["cp.cuda.ExternalStream"]:
        return None

    def destroy(self) -> None:
        pass

    def get_transport_name(self) -> str:
        return "nccl"


class MockNcclGroupSet:
    def __init__(self):
        # Represents a mapping from a NCCL group ID to a set of actors and a custom
        # NCCL group.
        self.ids_to_actors_and_custom_comms: Dict[
            str, Tuple[FrozenSet["ray.actor.ActorHandle"], Optional[Communicator]]
        ] = {}

    def __call__(
        self,
        actors: List["ray.actor.ActorHandle"],
        custom_nccl_group: Optional[Communicator] = None,
        use_communication_streams: bool = False,
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
            ctx.communicators[group_id] = custom_nccl_group
        else:
            ctx.communicators[group_id] = AbstractNcclGroup(actors)

        return group_id

    def mock_destroy_nccl_group(self, group_id: str) -> None:
        ctx = ChannelContext.get_current()
        if group_id not in ctx.communicators:
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
        ctx.communicators[group_id].destroy()
        del ctx.communicators[group_id]

    def check_teardown(self, nccl_group_ids: List[str]) -> None:
        ctx = ChannelContext.get_current()
        for nccl_group_id in nccl_group_ids:
            assert nccl_group_id not in self.ids_to_actors_and_custom_comms
            assert nccl_group_id not in ctx.communicators


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
    custom_nccl_group: Optional[Communicator],
) -> None:
    ctx = ChannelContext.get_current()
    if custom_nccl_group is None:
        nccl_group = AbstractNcclGroup(actors)
        nccl_group.initialize(rank)
        ctx.communicators[group_id] = nccl_group
    else:
        custom_nccl_group.initialize(rank)
        ctx.communicators[group_id] = custom_nccl_group


def mock_do_destroy_nccl_group(self, group_id: str) -> None:
    ctx = ChannelContext.get_current()
    if group_id not in ctx.communicators:
        return
    ctx.communicators[group_id].destroy()
    del ctx.communicators[group_id]


def check_nccl_group_init(
    monkeypatch,
    dag: "ray.dag.DAGNode",
    actors_and_custom_comms: Set[
        Tuple[FrozenSet["ray.actor.ActorHandle"], Optional[Communicator]]
    ],
) -> "ray.dag.CompiledDAG":
    mock_nccl_group_set = MockNcclGroupSet()
    monkeypatch.setattr(
        "ray.dag.compiled_dag_node._init_communicator",
        mock_nccl_group_set,
    )

    compiled_dag = dag.experimental_compile()
    assert (
        set(mock_nccl_group_set.ids_to_actors_and_custom_comms.values())
        == actors_and_custom_comms
    )

    return compiled_dag, mock_nccl_group_set


def check_nccl_group_teardown(
    monkeypatch,
    compiled_dag: "ray.dag.CompiledDAG",
    mock_nccl_group_set: MockNcclGroupSet,
):
    monkeypatch.setattr(
        "ray.dag.compiled_dag_node._destroy_communicator",
        mock_nccl_group_set.mock_destroy_nccl_group,
    )

    created_communicator_ids = compiled_dag._actors_to_created_communicator_id.values()
    compiled_dag.teardown()
    mock_nccl_group_set.check_teardown(created_communicator_ids)
