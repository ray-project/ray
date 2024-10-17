# coding: utf-8
import logging
import os
import sys
import uuid
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

import pytest
import ray
import ray.cluster_utils
import ray.experimental.collective as collective
import torch
from ray.dag import InputNode, MultiOutputNode
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

    def send(self, shape, dtype, value: int, send_tensor=True):
        if not send_tensor:
            return 1
        return torch.ones(shape, dtype=dtype, device=self.device) * value

    def recv(self, tensor):
        # Check that tensor got loaded to the correct device.
        assert tensor.device == self.device
        return (tensor[0].item(), tensor.shape, tensor.dtype)

    def compute_with_tuple_args(self, args, i: int):
        shape, dtype, value = args[i]
        tensor = torch.ones(shape, dtype=dtype, device=self.device) * value
        return tensor


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
        nccl_group_id = str(uuid.uuid4())
        self.ids_to_actors_and_custom_comms[nccl_group_id] = (
            frozenset(actors),
            custom_nccl_group,
        )
        return nccl_group_id

    def check_init(
        self,
        compiled_dag: "ray.dag.CompiledDAG",
        num: int,
        actors_and_custom_comms: Set[
            Tuple[FrozenSet["ray.actor.ActorHandle"], Optional[GPUCommunicator]]
        ],
        p2p_actors_and_custom_comm: Optional[
            Tuple[FrozenSet["ray.actor.ActorHandle"], Optional[GPUCommunicator]]
        ],
    ) -> None:
        assert len(self.ids_to_actors_and_custom_comms) == num
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


def check_nccl_group_init(
    monkeypatch,
    dag: "ray.dag.DAGNode",
    num: int,
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
        num,
        actors_and_custom_comms,
        p2p_actors_and_custom_comm,
    )

    return compiled_dag


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_all_reduce_duplicate_actors(ray_start_regular):
    """
    Test an error is thrown when two input nodes from the same actor bind to
    an all-reduce.
    """
    actor_cls = CPUTorchTensorWorker.options()
    worker = actor_cls.remote()

    with InputNode() as inp:
        computes = [worker.compute_with_tuple_args.bind(inp, 0) for _ in range(2)]
        with pytest.raises(
            ValueError,
            match="Expected unique actor handles for a collective operation",
        ):
            collective.allreduce.bind(computes)

    with InputNode() as inp:
        compute = worker.compute_with_tuple_args.bind(inp, 0)
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

    class AbstractNcclGroup(GPUCommunicator):
        """
        A dummy NCCL group for testing.
        """

        def __init__(self, actor_handles):
            self._actor_handles = actor_handles

        def initialize(self, rank: int) -> None:
            pass

        def get_rank(self, actor: ray.actor.ActorHandle) -> int:
            raise NotImplementedError

        def get_world_size(self) -> int:
            raise NotImplementedError

        def get_self_rank(self) -> Optional[int]:
            raise NotImplementedError

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
            raise NotImplementedError

    nccl_group = AbstractNcclGroup([workers[0]])
    with InputNode() as inp:
        computes = [
            worker.compute_with_tuple_args.bind(inp, i)
            for i, worker in enumerate(workers)
        ]
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
        computes = [
            worker.compute_with_tuple_args.bind(inp, i)
            for i, worker in enumerate(workers)
        ]
        collectives = [collective.allreduce.bind([compute]) for compute in computes]
        recvs = [
            # Note: There are two all-reduces, each on one actor.
            # collective[0] is the only CollectiveOutputNode for each all-reduce.
            worker.recv.bind(collective[0])
            for worker, collective in zip(workers, collectives)
        ]
        dag = MultiOutputNode(recvs)

    compiled_dag = check_nccl_group_init(
        monkeypatch,
        dag,
        2,
        {
            (frozenset([workers[0]]), None),
            (frozenset([workers[1]]), None),
        },
    )

    compiled_dag.teardown()


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
        computes = [
            worker.compute_with_tuple_args.bind(inp, i)
            for i, worker in enumerate(workers)
        ]
        collectives = collective.allreduce.bind(computes)
        collectives = collective.allreduce.bind(collectives)
        recvs = [
            worker.recv.bind(collective)
            for worker, collective in zip(workers, collectives)
        ]
        dag = MultiOutputNode(recvs)

    compiled_dag = check_nccl_group_init(
        monkeypatch,
        dag,
        1,
        {(frozenset(workers), None)},
    )

    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
