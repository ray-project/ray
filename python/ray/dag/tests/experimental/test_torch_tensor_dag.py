# coding: utf-8
import logging
import os
import socket
import sys
import time
from typing import FrozenSet, List, Optional, Set, Tuple

import pytest
import ray
import ray.cluster_utils
import ray.experimental.collective as collective
import torch
from ray.air._internal import torch_utils
from ray.dag import InputNode, MultiOutputNode
from ray.exceptions import RayChannelError
from ray.experimental.channel import ChannelContext
from ray.experimental.channel.gpu_communicator import (
    GPUCommunicator,
    TorchTensorAllocator,
)
from ray.experimental.channel.nccl_group import _NcclGroup
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.tests.conftest import *  # noqa
from ray.util.collective.types import ReduceOp

logger = logging.getLogger(__name__)

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))


@ray.remote
class TorchTensorWorker:
    def __init__(self, use_gpu: bool = True):
        if use_gpu:
            self.device = torch_utils.get_devices()[0]
        else:
            self.device = "cpu"

    def init_distributed(self, world_size, rank):
        torch.distributed.init_process_group(
            backend="nccl", world_size=world_size, rank=rank
        )

    def send(self, shape, dtype, value: int, send_tensor=True):
        if not send_tensor:
            return 1
        return torch.ones(shape, dtype=dtype, device=self.device) * value

    def send_or_raise(self, shape, dtype, value: int, raise_exception=False):
        if raise_exception:
            raise RuntimeError()
        return torch.ones(shape, dtype=dtype, device=self.device) * value

    def send_dict_with_tuple_args(self, args):
        shape, dtype, value = args
        return {
            i: torch.ones(shape, dtype=dtype, device=self.device) * i
            for i in range(value)
        }

    def send_with_tuple_args(self, args):
        # Hack because InputNode can currently only contain one arg.
        shape, dtype, value = args
        return torch.ones(shape, dtype=dtype, device=self.device) * value

    def recv(self, tensor):
        # Check that tensor got loaded to the correct device.
        assert tensor.device == self.device
        return (tensor[0].item(), tensor.shape, tensor.dtype)

    def recv_dict(self, tensor_dict):
        vals = {}
        for i, tensor in tensor_dict.items():
            vals[i] = self.recv(tensor)
        return vals

    def compute_with_tuple_args(self, args, i: int):
        shape, dtype, value = args[i]
        tensor = torch.ones(shape, dtype=dtype, device=self.device) * value
        return tensor

    def recv_tensor(self, tensor):
        assert tensor.device == self.device
        return tensor

    def ping(self):
        return


@ray.remote(num_cpus=1)
class TrainWorker:
    """
    This class simulates a training worker.
    """

    def __init__(self):
        pass

    def entrypoint(self, inp):
        pass

    def forward(self, inp):
        return torch.randn(10, 10)


class TestNcclGroup(GPUCommunicator):
    """
    A custom NCCL group for testing. This is a simple wrapper around `_NcclGroup`.
    """

    def __init__(self, world_size, comm_id, actor_handles):
        self._world_size = world_size
        self._comm_id = comm_id
        self._actor_handles = actor_handles
        self._inner = None

    def initialize(self, rank: int) -> None:
        self._inner = _NcclGroup(
            self._world_size,
            self._comm_id,
            rank,
            self._actor_handles,
            torch.cuda.current_stream().cuda_stream,
        )

    def get_rank(self, actor: ray.actor.ActorHandle) -> int:
        # Implement this without forwarding to `_inner` to allow the method
        # to be called before initialization.
        actor_ids = [a._ray_actor_id for a in self._actor_handles]
        try:
            rank = actor_ids.index(actor._ray_actor_id)
        except ValueError:
            raise ValueError("Actor is not in the NCCL group.")
        return rank

    def get_world_size(self) -> int:
        # Implement this without forwarding to `_inner` to allow the method
        # to be called before initialization.
        return self._world_size

    def get_self_rank(self) -> Optional[int]:
        if self._inner is None:
            return None
        return self._inner.get_self_rank()

    def get_actor_handles(self) -> List["ray.actor.ActorHandle"]:
        return self._actor_handles

    def send(self, value: "torch.Tensor", peer_rank: int) -> None:
        return self._inner.send(value, peer_rank)

    def recv(
        self,
        shape: Tuple[int],
        dtype: "torch.dtype",
        peer_rank: int,
        allocator: Optional[TorchTensorAllocator] = None,
    ) -> "torch.Tensor":
        return self._inner.recv(shape, dtype, peer_rank, allocator=allocator)

    def allreduce(
        self,
        send_buf: "torch.Tensor",
        recv_buf: "torch.Tensor",
        op: ReduceOp = ReduceOp.SUM,
    ) -> None:
        return self._inner.allreduce(send_buf, recv_buf, op)

    def destroy(self) -> None:
        return self._inner.destroy()


def check_nccl_group_init(
    compiled_dag: "ray.dag.CompiledDAG",
    num: int,
    actors_and_custom_comms: Set[
        Tuple[FrozenSet["ray.actor.ActorHandle"], Optional[GPUCommunicator]]
    ],
    p2p_actors_and_custom_comm: Optional[
        Tuple[FrozenSet["ray.actor.ActorHandle"], Optional[GPUCommunicator]]
    ],
) -> None:
    nccl_group_ids = compiled_dag.nccl_group_ids

    ctx = ChannelContext.get_current()
    ctx_actors_and_custom_comms = set()
    for nccl_group_id in nccl_group_ids:
        nccl_group = ctx.nccl_groups[nccl_group_id]
        if isinstance(nccl_group, _NcclGroup):
            custom_nccl_group = None
        else:
            custom_nccl_group = nccl_group
        ctx_actors_and_custom_comms.add(
            (frozenset(nccl_group.get_actor_handles()), custom_nccl_group)
        )
    assert ctx_actors_and_custom_comms == actors_and_custom_comms

    nccl_group_id_p2p = compiled_dag.nccl_group_id_p2p
    if p2p_actors_and_custom_comm is None:
        assert nccl_group_id_p2p is None
    else:
        assert nccl_group_id_p2p
        nccl_group = ctx.nccl_groups[nccl_group_id_p2p]
        actors, custom_nccl_group = p2p_actors_and_custom_comm
        if isinstance(nccl_group, _NcclGroup):
            assert custom_nccl_group is None
        else:
            assert nccl_group == custom_nccl_group
        assert set(nccl_group.get_actor_handles()).issuperset(actors)


def check_nccl_group_teardown(compiled_dag: "ray.dag.CompiledDAG") -> None:
    nccl_group_ids = compiled_dag.nccl_group_ids
    ctx = ChannelContext.get_current()
    for nccl_group_id in nccl_group_ids:
        assert nccl_group_id not in ctx.nccl_groups


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_p2p(ray_start_regular):
    if USE_GPU:
        assert sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 0

    actor_cls = TorchTensorWorker
    if USE_GPU:
        actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        # TODO(swang): Test that we are using the minimum number of
        # channels/messages when _direct_return=True.
        dag = dag.with_type_hint(TorchTensorType(shape, dtype, _direct_return=True))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        assert result == (i, shape, dtype)

    compiled_dag.teardown()

    # Passing tensors of a similar or smaller shape is okay.
    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        # TODO(swang): Test that we are using the minimum number of
        # channels/messages when _direct_return=True.
        dag = dag.with_type_hint(TorchTensorType((20,), dtype, _direct_return=True))
        dag = receiver.recv.bind(dag)
    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        assert result == (i, shape, dtype)
    compiled_dag.teardown()

    # Passing a torch.tensor inside of other data is okay even if
    # _direct_return=True, if `transport` is not set.
    with InputNode() as inp:
        dag = sender.send_dict_with_tuple_args.bind(inp)
        dag = dag.with_type_hint(
            TorchTensorType(
                _shape=shape,
                _dtype=dtype,
                _direct_return=True,
            )
        )
        dag = receiver.recv_dict.bind(dag)

    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute((shape, dtype, 1))
    ray.get(ref)
    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_as_dag_input(ray_start_regular):
    if USE_GPU:
        assert sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 0

    actor_cls = TorchTensorWorker
    if USE_GPU:
        actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    receiver = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor as input.
    with InputNode() as inp:
        # TODO(swang): Test that we are using the minimum number of
        # channels/messages when _direct_return=True.
        torch_inp = inp.with_type_hint(
            TorchTensorType(shape, dtype, _direct_return=True)
        )
        dag = receiver.recv.bind(torch_inp)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(torch.ones(shape, dtype=dtype) * i)
        result = ray.get(ref)
        assert result == (i, shape, dtype)

    # Passing tensors of a similar or smaller shape is okay.
    for i in range(3):
        ref = compiled_dag.execute(torch.ones((20,), dtype=dtype) * i)
        result = ray.get(ref)
        assert result == (i, (20,), dtype)

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        # TODO(swang): Test that we are using the minimum number of
        # channels/messages when _direct_return=True.
        dag = dag.with_type_hint(
            TorchTensorType(shape, dtype, transport="nccl", _direct_return=True)
        )
        dag = receiver.recv.bind(dag)

    # Test normal execution.
    compiled_dag = dag.experimental_compile()

    for i in range(3):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        assert result == (i, shape, dtype)

    compiled_dag.teardown()

    # Test that InputNode cannot cannot participate in the NCCL group.
    with InputNode() as inp:
        torch_inp = inp.with_type_hint(TorchTensorType(shape, dtype, transport="nccl"))
        dag = receiver.recv.bind(torch_inp)
    with pytest.raises(
        ValueError,
        match=(
            r"DAG inputs cannot be transferred "
            "via NCCL because the driver cannot participate in the NCCL group"
        ),
    ):
        compiled_dag = dag.experimental_compile()

    # Test that actors can be reused for a valid DAG.
    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        dag = dag.with_type_hint(TorchTensorType(shape, dtype, transport="nccl"))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        assert result == (i, shape, dtype)
    compiled_dag.teardown()

    # TODO(swang): Check that actors are still alive. Currently this fails due
    # to a ref counting assertion error.
    # ray.get(sender.ping.remote())
    # ray.get(receiver.ping.remote())


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_dynamic(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    with InputNode() as inp:
        dag = sender.send_with_tuple_args.bind(inp)
        # TODO(swang): Test that we are using the minimum number of
        # channels/messages when _direct_return=True.
        dag = dag.with_type_hint(TorchTensorType(transport="nccl", _direct_return=True))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        i += 1
        shape = (i * 10,)
        dtype = torch.float16
        args = (shape, dtype, i)
        ref = compiled_dag.execute(args)
        result = ray.get(ref)
        assert result == (i, shape, dtype)

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_custom_comm(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    from cupy.cuda import nccl

    comm_id = nccl.get_unique_id()
    nccl_group = TestNcclGroup(2, comm_id, [sender, receiver])
    with InputNode() as inp:
        dag = sender.send_with_tuple_args.bind(inp)
        dag = dag.with_type_hint(TorchTensorType(transport=nccl_group))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        i += 1
        shape = (i * 10,)
        dtype = torch.float16
        args = (shape, dtype, i)
        ref = compiled_dag.execute(args)
        result = ray.get(ref)
        assert result == (i, shape, dtype)

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_custom_comm_invalid(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    actor1 = actor_cls.remote()
    actor2 = actor_cls.remote()

    class MockNcclGroup(GPUCommunicator):
        """
        A mock NCCL group for testing. Send and recv are not implemented.
        """

        def __init__(self, world_size, actor_handles):
            self._world_size = world_size
            self._actor_handles = actor_handles
            self._rank = None

        def initialize(self, rank: int) -> None:
            expected_rank = self.get_rank(ray.get_runtime_context().current_actor)
            assert (
                rank == expected_rank
            ), f"NCCL actor's rank {rank} does not match expected rank {expected_rank}"
            self._rank = rank
            self._device = torch_utils.get_devices()[0]

        def get_rank(self, actor: ray.actor.ActorHandle) -> int:
            actor_ids = [a._ray_actor_id for a in self._actor_handles]
            try:
                rank = actor_ids.index(actor._ray_actor_id)
            except ValueError:
                raise ValueError("Actor is not in the NCCL group.")
            return rank

        def get_world_size(self) -> int:
            return self._world_size

        def get_self_rank(self) -> Optional[int]:
            return self._rank

        def get_actor_handles(self) -> List["ray.actor.ActorHandle"]:
            return self._actor_handles

        def send(self, value: "torch.Tensor", peer_rank: int) -> None:
            return None

        def recv(
            self,
            shape: Tuple[int],
            dtype: "torch.dtype",
            peer_rank: int,
            allocator: Optional[TorchTensorAllocator] = None,
        ) -> "torch.Tensor":
            return None

        def allreduce(
            self,
            send_buf: "torch.Tensor",
            recv_buf: "torch.Tensor",
            op: ReduceOp,
        ) -> None:
            raise NotImplementedError

        def destroy(self) -> None:
            pass

    nccl_group = MockNcclGroup(2, [actor1, actor2])

    # Mixed usage of NCCL groups should throw an error
    # Case 1: custom NCCL group first, then default NCCL group
    with InputNode() as inp:
        dag = actor1.send_with_tuple_args.bind(inp)
        dag = dag.with_type_hint(TorchTensorType(transport=nccl_group))
        dag = actor2.recv.bind(dag)
        dag = actor2.send_with_tuple_args.bind(dag)
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = actor1.recv.bind(dag)
    with pytest.raises(
        ValueError,
        match=r"Accelerated DAGs do not support mixed usage of type hints.*",
    ):
        dag.experimental_compile()

    # Case 2: default NCCL group first, then custom NCCL group
    with InputNode() as inp:
        dag = actor1.send_with_tuple_args.bind(inp)
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = actor2.recv.bind(dag)
        dag = actor2.send_with_tuple_args.bind(dag)
        dag = dag.with_type_hint(TorchTensorType(transport=nccl_group))
        dag = actor1.recv.bind(dag)
    with pytest.raises(
        ValueError,
        match=r"Accelerated DAGs do not support mixed usage of type hints.*",
    ):
        dag.experimental_compile()

    nccl_group2 = MockNcclGroup(2, [actor1, actor2])

    # Using two different custom NCCL groups are currently not supported
    with InputNode() as inp:
        dag = actor1.send_with_tuple_args.bind(inp)
        dag = dag.with_type_hint(TorchTensorType(transport=nccl_group))
        dag = actor2.recv.bind(dag)
        dag = actor2.send_with_tuple_args.bind(dag)
        dag = dag.with_type_hint(TorchTensorType(transport=nccl_group2))
        dag = actor1.recv.bind(dag)
    with pytest.raises(
        ValueError,
        match=(
            "Accelerated DAGs currently only support "
            "a single custom NCCL group, but multiple "
            "have been specified."
        ),
    ):
        dag.experimental_compile()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_custom_comm_inited(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"
    runtime_env = {
        "env_vars": {
            "MASTER_ADDR": socket.gethostbyname(socket.gethostname()),
            "MASTER_PORT": "8888",
        }
    }
    actor_cls = TorchTensorWorker.options(
        num_cpus=0, num_gpus=1, runtime_env=runtime_env
    )

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    # Simulates that the distributed environment (e.g., torch.distributed)
    # have already been set up
    refs = [
        sender.init_distributed.remote(2, 0),
        receiver.init_distributed.remote(2, 1),
    ]
    ray.wait(refs)

    class InitedNcclGroup(GPUCommunicator):
        """
        A custom NCCL group based on existing torch.distributed setup.
        """

        def __init__(self, world_size, actor_handles):
            self._world_size = world_size
            self._actor_handles = actor_handles
            self._rank = None

        def initialize(self, rank: int) -> None:
            expected_rank = self.get_rank(ray.get_runtime_context().current_actor)
            assert (
                rank == expected_rank
            ), f"NCCL actor's rank {rank} does not match expected rank {expected_rank}"
            self._rank = rank
            self._device = torch_utils.get_devices()[0]

        def get_rank(self, actor: ray.actor.ActorHandle) -> int:
            actor_ids = [a._ray_actor_id for a in self._actor_handles]
            try:
                rank = actor_ids.index(actor._ray_actor_id)
            except ValueError:
                raise ValueError("Actor is not in the NCCL group.")
            return rank

        def get_world_size(self) -> int:
            return self._world_size

        def get_self_rank(self) -> Optional[int]:
            return self._rank

        def get_actor_handles(self) -> List["ray.actor.ActorHandle"]:
            return self._actor_handles

        def send(self, value: "torch.Tensor", peer_rank: int) -> None:
            torch.distributed.send(value, peer_rank)

        def recv(
            self,
            shape: Tuple[int],
            dtype: "torch.dtype",
            peer_rank: int,
            allocator: Optional[TorchTensorAllocator] = None,
        ) -> "torch.Tensor":
            tensor = torch.empty(torch.Size(shape), dtype=dtype, device=self._device)
            torch.distributed.recv(tensor, peer_rank)
            return tensor

        def allreduce(
            self,
            send_buf: "torch.Tensor",
            recv_buf: "torch.Tensor",
            op: ReduceOp,
        ) -> None:
            raise NotImplementedError

        def destroy(self) -> None:
            pass

    nccl_group = InitedNcclGroup(2, [sender, receiver])

    with InputNode() as inp:
        dag = sender.send_with_tuple_args.bind(inp)
        dag = dag.with_type_hint(TorchTensorType(transport=nccl_group))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        i += 1
        shape = (i * 10,)
        dtype = torch.float16
        args = (shape, dtype, i)
        ref = compiled_dag.execute(args)
        result = ray.get(ref)
        assert result == (i, shape, dtype)

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_wrong_shape(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    dtype = torch.float16

    # Passing tensors of the wrong shape will error.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value)
        dag = dag.with_type_hint(
            TorchTensorType(
                (20,),
                dtype,
                transport="nccl",
            )
        )
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute(shape=(20,), dtype=dtype, value=1)
    ray.get(ref) == (1, (20,), dtype)

    ref = compiled_dag.execute(shape=(10,), dtype=dtype, value=1)

    with pytest.raises(RayChannelError):
        ray.get(ref)

    # For tensors where the shape is declared to be static, the DAG will be
    # torn down after any task throws an application-level exception, such as
    # when the task returns torch.Tensors of the wrong shape or dtype. Check
    # that we can no longer submit to the DAG.
    with pytest.raises(RayChannelError):
        ref = compiled_dag.execute(shape=(20,), dtype=dtype, value=1)

    compiled_dag.teardown()

    # TODO(swang): This currently requires time.sleep to avoid some issue with
    # following tests.
    time.sleep(3)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_nested(ray_start_regular):
    """
    Test nested torch.Tensor passed via NCCL. Its shape and dtype is statically
    declared.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    with InputNode() as inp:
        dag = sender.send_dict_with_tuple_args.bind(inp)
        dag = dag.with_type_hint(
            TorchTensorType(_shape=shape, _dtype=dtype, transport="nccl")
        )
        dag = receiver.recv_dict.bind(dag)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        args = (shape, dtype, 1)

        ref = compiled_dag.execute(args)
        result = ray.get(ref)
        expected_result = {0: (0, shape, dtype)}
        assert result == expected_result

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_nested_dynamic(ray_start_regular):
    """
    Test nested torch.Tensor passed via NCCL. Its shape and dtype is
    dynamically declared, and there may be multiple tensors.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    with InputNode() as inp:
        dag = sender.send_dict_with_tuple_args.bind(inp)
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = receiver.recv_dict.bind(dag)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        i += 1

        shape = (10 * i,)
        dtype = torch.float16
        args = (shape, dtype, i)

        ref = compiled_dag.execute(args)
        result = ray.get(ref)
        expected_result = {j: (j, shape, dtype) for j in range(i)}
        assert result == expected_result

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_direct_return_error(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    # Passing a non-tensor value when _direct_return=True and tranport="nccl"
    # fails.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value, inp.send_tensor)
        dag = dag.with_type_hint(
            TorchTensorType(
                transport=TorchTensorType.NCCL,
                _direct_return=True,
            )
        )
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute(shape=shape, dtype=dtype, value=1, send_tensor=True)
    assert ray.get(ref) == (1, shape, dtype)

    ref = compiled_dag.execute(shape=shape, dtype=dtype, value=1, send_tensor=False)
    with pytest.raises(RayChannelError):
        ray.get(ref)

    # For direct_return=True tensors, the DAG will be torn down after any task
    # throws an application-level exception, such as when the task returns
    # something other than a torch.Tensor. Check that we can no longer submit
    # to the DAG.
    with pytest.raises(RayChannelError):
        ref = compiled_dag.execute(shape=shape, dtype=dtype, value=1, send_tensor=True)

    compiled_dag.teardown()

    # TODO(swang): This currently requires time.sleep to avoid some issue with
    # following tests.
    time.sleep(3)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_exceptions(ray_start_regular):
    """
    Test nested torch.Tensor passed via NCCL. Its shape and dtype is
    dynamically declared, and there may be multiple tensors.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    with InputNode() as inp:
        dag = sender.send_or_raise.bind(
            inp.shape, inp.dtype, inp.value, inp.raise_exception
        )
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        i += 1

        shape = (10 * i,)
        dtype = torch.float16

        ref = compiled_dag.execute(
            shape=shape,
            dtype=dtype,
            value=i,
            raise_exception=False,
        )
        result = ray.get(ref)
        assert result == (i, shape, dtype)

    # Application level exceptions are thrown to the end ray.get
    ref = compiled_dag.execute(
        shape=shape,
        dtype=dtype,
        value=i,
        raise_exception=True,
    )
    with pytest.raises(RuntimeError):
        ray.get(ref)

    # If using dynamic shape or dtype is used and direct_return=False, then the
    # DAG should still be usable after application-level exceptions.
    ref = compiled_dag.execute(
        shape=shape,
        dtype=dtype,
        value=i,
        raise_exception=False,
    )
    result = ray.get(ref)
    assert result == (i, shape, dtype)

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_all_reduce(ray_start_regular):
    """
    Test basic all-reduce.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    with InputNode() as inp:
        computes = [
            worker.compute_with_tuple_args.bind(inp, i)
            for i, worker in enumerate(workers)
        ]
        collectives = collective.allreduce.bind(computes, ReduceOp.SUM)
        recvs = [
            worker.recv.bind(collective)
            for worker, collective in zip(workers, collectives)
        ]
        dag = MultiOutputNode(recvs)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        i += 1
        shape = (i * 10,)
        dtype = torch.float16
        ref = compiled_dag.execute(
            [(shape, dtype, i + idx) for idx in range(num_workers)]
        )
        result = ray.get(ref)
        reduced_val = sum(i + idx for idx in range(num_workers))
        assert result == [(reduced_val, shape, dtype) for _ in workers]

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_all_reduce_get_partial(ray_start_regular):
    """
    Test getting partial results from an all-reduce does not hang.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    shape = (10,)
    dtype = torch.float16

    with InputNode() as inp:
        computes = [
            worker.compute_with_tuple_args.bind(inp, i)
            for i, worker in enumerate(workers)
        ]
        collectives = collective.allreduce.bind(computes, ReduceOp.SUM)
        recv = workers[0].recv.bind(collectives[0])
        tensor = workers[1].recv_tensor.bind(collectives[0])
        dag = MultiOutputNode([recv, tensor])

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        ref = compiled_dag.execute(
            [(shape, dtype, i + idx + 1) for idx in range(num_workers)]
        )
        result = ray.get(ref)
        metadata, tensor = result
        reduced_val = sum(i + idx + 1 for idx in range(num_workers))
        assert metadata == (reduced_val, shape, dtype)
        tensor = tensor.to("cpu")
        expected_tensor_val = torch.ones(shape, dtype=dtype) * reduced_val
        assert torch.equal(tensor, expected_tensor_val)

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_all_reduce_allocate_tensor(ray_start_regular):
    """
    Test a new tensor is allocated before all-reduce and the input tensor can
    be reused.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    shape = (10,)
    dtype = torch.float16
    with InputNode() as inp:
        x = workers[0].send.bind(shape, dtype, inp)
        y = workers[1].send.bind(shape, dtype, inp)
        allreduce = collective.allreduce.bind([x, y])
        u = workers[1].recv_tensor.bind(x)
        v = workers[0].recv_tensor.bind(y)
        dag = MultiOutputNode([*allreduce, u, v])

    compiled_dag = dag.experimental_compile()

    value = 10
    ref = compiled_dag.execute(value)
    result = ray.get(ref)
    result = [tensor.to("cpu") for tensor in result]
    expected_reduced_tensor_val = torch.ones(shape, dtype=dtype) * value * 2
    expected_original_tensor_val = torch.ones(shape, dtype=dtype) * value
    assert torch.equal(result[0], expected_reduced_tensor_val)
    assert torch.equal(result[1], expected_reduced_tensor_val)
    assert torch.equal(result[2], expected_original_tensor_val)
    assert torch.equal(result[3], expected_original_tensor_val)

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_all_reduce_wrong_shape(ray_start_regular):
    """
    Test an error is thrown when an all-reduce takes tensors of wrong shapes.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    dtype = torch.float16

    with InputNode() as inp:
        computes = [
            worker.compute_with_tuple_args.bind(inp, i)
            for i, worker in enumerate(workers)
        ]
        collectives = collective.allreduce.bind(computes, ReduceOp.SUM)
        recvs = [
            worker.recv.bind(collective)
            for worker, collective in zip(workers, collectives)
        ]
        dag = MultiOutputNode(recvs)

    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute([((20,), dtype, idx + 1) for idx in range(num_workers)])
    reduced_val = (1 + num_workers) * num_workers / 2
    assert ray.get(ref) == [(reduced_val, (20,), dtype) for _ in range(num_workers)]

    ref = compiled_dag.execute(
        [((10 * (idx + 1),), dtype, idx + 1) for idx in range(num_workers)]
    )
    # Execution hangs because of shape mismatch and a timeout error is raised.
    with pytest.raises(RayChannelError):
        ray.get(ref)

    # The DAG will be torn down after any task throws an application-level
    # exception, such as when the task returns torch.Tensors of the wrong
    # shape or dtype. Check that we can no longer submit to the DAG.
    ref = compiled_dag.execute([((20,), dtype, 1) for _ in workers])
    with pytest.raises(RayChannelError):
        ref = compiled_dag.execute([((20,), dtype, 1) for _ in workers])

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_all_reduce_custom_comm(ray_start_regular):
    """
    Test all-reduce works with a custom communicator.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    from cupy.cuda import nccl

    comm_id = nccl.get_unique_id()
    nccl_group = TestNcclGroup(2, comm_id, workers)
    with InputNode() as inp:
        computes = [
            worker.compute_with_tuple_args.bind(inp, i)
            for i, worker in enumerate(workers)
        ]
        collectives = collective.allreduce.bind(computes, transport=nccl_group)
        recvs = [
            worker.recv.bind(collective)
            for worker, collective in zip(workers, collectives)
        ]
        dag = MultiOutputNode(recvs)

    compiled_dag = dag.experimental_compile()

    shape = (10,)
    dtype = torch.float16
    for i in range(3):
        ref = compiled_dag.execute(
            [(shape, dtype, i + idx + 1) for idx in range(num_workers)]
        )
        result = ray.get(ref)
        reduced_val = sum(i + idx + 1 for idx in range(num_workers))
        assert result == [(reduced_val, shape, dtype) for _ in workers]

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 4}], indirect=True)
def test_torch_tensor_nccl_comm_deduplicate_p2p_and_collective(
    ray_start_regular, monkeypatch
):
    """
    Test communicators are deduplicated when the collective and the P2P are
    on the same set of actors.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    with InputNode() as inp:
        computes = [
            worker.compute_with_tuple_args.bind(inp, i)
            for i, worker in enumerate(workers)
        ]
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

    compiled_dag = dag.experimental_compile()
    check_nccl_group_init(
        compiled_dag,
        1,
        {(frozenset(workers), None)},
        (frozenset(workers), None),
    )

    # Sanity check: the compiled dag can execute.
    shape = (10,)
    dtype = torch.float16
    ref = compiled_dag.execute([(shape, dtype, i + 1) for i in range(num_workers)])
    result = ray.get(ref)
    reduced_val = sum(i + 1 for i in range(num_workers))
    assert result == [(reduced_val, shape, dtype) for _ in workers]

    compiled_dag.teardown()

    with InputNode() as inp:
        computes = [
            worker.compute_with_tuple_args.bind(inp, i)
            for i, worker in enumerate(workers)
        ]
        collectives = collective.allreduce.bind(computes)
        # Sender is workers[0] and receiver is workers[1].
        dag = workers[1].recv.bind(
            collectives[0].with_type_hint(TorchTensorType(transport="nccl"))
        )

    compiled_dag = dag.experimental_compile()
    check_nccl_group_init(
        compiled_dag,
        1,
        {(frozenset(workers), None)},
        (frozenset(workers), None),
    )

    # Sanity check: the compiled dag can execute.
    shape = (10,)
    dtype = torch.float16
    ref = compiled_dag.execute([(shape, dtype, i + 1) for i in range(num_workers)])
    result = ray.get(ref)
    reduced_val = sum(i + 1 for i in range(num_workers))
    assert result == (reduced_val, shape, dtype)

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_custom_comm_deduplicate(ray_start_regular, monkeypatch):
    """
    Test a custom GPU communicator is reused when possible.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    from cupy.cuda import nccl

    comm_id = nccl.get_unique_id()
    comm = TestNcclGroup(num_workers, comm_id, workers)
    with InputNode() as inp:
        computes = [
            worker.compute_with_tuple_args.bind(inp, i)
            for i, worker in enumerate(workers)
        ]
        collectives = collective.allreduce.bind(computes, transport=comm)
        collectives = collective.allreduce.bind(collectives)
        dag = workers[0].recv.bind(
            collectives[1].with_type_hint(TorchTensorType(transport="nccl"))
        )

    compiled_dag = dag.experimental_compile()
    check_nccl_group_init(
        compiled_dag,
        1,
        {(frozenset(workers), comm)},
        (frozenset(workers), comm),
    )

    compiled_dag.teardown()

    comm_id = nccl.get_unique_id()
    comm = TestNcclGroup(num_workers, comm_id, workers)
    with InputNode() as inp:
        computes = [
            worker.compute_with_tuple_args.bind(inp, i)
            for i, worker in enumerate(workers)
        ]
        collectives = collective.allreduce.bind(computes)
        collectives = collective.allreduce.bind(collectives)
        dag = workers[0].recv.bind(
            collectives[1].with_type_hint(TorchTensorType(transport=comm))
        )

    compiled_dag = dag.experimental_compile()
    check_nccl_group_init(
        compiled_dag,
        1,
        {(frozenset(workers), comm)},
        (frozenset(workers), comm),
    )

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 4}], indirect=True)
def test_torch_tensor_nccl_custom_comm_init_teardown(ray_start_regular, monkeypatch):
    """
    Test custom NCCL groups are properly initialized and destroyed.
    1. Test when multiple type hints have the same `transport=custom_nccl_group`,
    the `custom_nccl_group` is initialized only once.
    2. Test all initialized NCCL groups are destroyed during teardown.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    from cupy.cuda import nccl

    comm_id = nccl.get_unique_id()
    comm = TestNcclGroup(num_workers, comm_id, workers)

    shape = (10,)
    dtype = torch.float16
    with InputNode() as inp:
        tensors = [worker.send.bind(shape, dtype, inp) for worker in workers]
        allreduce = collective.allreduce.bind(tensors, transport=comm)
        dag = workers[0].recv.bind(
            allreduce[1].with_type_hint(TorchTensorType(transport=comm))
        )

    compiled_dag = dag.experimental_compile()
    check_nccl_group_init(
        compiled_dag,
        1,
        {(frozenset(workers), comm)},
        (frozenset(workers), comm),
    )

    # Sanity check: the compiled dag can execute.
    value = 10
    ref = compiled_dag.execute(value)
    result = ray.get(ref)
    assert result == (value * num_workers, shape, dtype)

    compiled_dag.teardown()
    check_nccl_group_teardown(compiled_dag)

    comm_id_1 = nccl.get_unique_id()
    comm_1 = TestNcclGroup(num_workers, comm_id_1, workers)
    comm_id_2 = nccl.get_unique_id()
    comm_2 = TestNcclGroup(num_workers, comm_id_2, workers)
    comm_id_3 = nccl.get_unique_id()
    comm_3 = TestNcclGroup(num_workers, comm_id_3, workers)

    with InputNode() as inp:
        tensors = [worker.send.bind(shape, dtype, inp) for worker in workers]
        allreduce1 = collective.allreduce.bind(tensors, transport=comm_1)
        allreduce2 = collective.allreduce.bind(allreduce1, transport=comm_2)
        dag = workers[0].recv.bind(
            allreduce2[1].with_type_hint(TorchTensorType(transport=comm_3))
        )

    compiled_dag = dag.experimental_compile()
    check_nccl_group_init(
        compiled_dag,
        3,
        {
            (frozenset(workers), comm_1),
            (frozenset(workers), comm_2),
            (frozenset(workers), comm_3),
        },
        (frozenset(workers), comm_3),
    )

    # Sanity check: the compiled dag can execute.
    value = 20
    ref = compiled_dag.execute(value)
    result = ray.get(ref)
    assert result == (value * num_workers * 2, shape, dtype)

    compiled_dag.teardown()
    check_nccl_group_teardown(compiled_dag)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_all_reduce_scheduling(ray_start_regular):
    """
    Test scheduling avoids potential deadlocks that arise from all-reduce operations.

    inp --> x(0) --> +------------+
        |            | all-reduce |
        --> y(1) --> +------------+
        |
        --> t(0) --> recv(1)

    In the above graph, x, y, t are tensors, and the numbers inside parentheses
    identify the actors. If actor 1 launches an all-reduce with tensor y while
    actor 0 starts sending t, then actor 1 waits for actor 0 to join the all-reduce
    while actor 1 waits for actor 0 to receive t.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    num_workers = 2
    workers = [actor_cls.remote() for _ in range(num_workers)]

    shape = (10,)
    dtype = torch.float16
    with InputNode() as inp:
        # Tensors in the all-reduce.
        x = workers[0].send.bind(shape, dtype, inp)
        y = workers[1].send.bind(shape, dtype, inp)

        # Tensor to be sent from workes[0] to workers[1].
        t = workers[0].send.bind(shape, dtype, inp)
        t.with_type_hint(TorchTensorType(transport="nccl"))

        collectives = collective.allreduce.bind([x, y])
        recv = workers[1].recv.bind(t)
        dag = MultiOutputNode([collectives[0], collectives[1], recv])

    compiled_dag = dag.experimental_compile()

    value = 10
    ref = compiled_dag.execute(value)
    result = ray.get(ref)
    reduced_value = value * 2
    expected_tensor_val = torch.ones(shape, dtype=dtype) * reduced_value
    assert torch.equal(result[0], expected_tensor_val)
    assert torch.equal(result[1], expected_tensor_val)
    assert result[2] == (value, shape, dtype)

    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
