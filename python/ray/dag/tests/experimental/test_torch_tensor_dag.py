# coding: utf-8
import logging
import os
import socket
import sys
from typing import List, Optional, Tuple

import pytest
import ray
import ray.cluster_utils
import ray.experimental.collective as collective
import torch
from ray.air._internal import torch_utils
from ray.dag import InputNode, MultiOutputNode
from ray.exceptions import RayChannelError
from ray.experimental.channel.gpu_communicator import (
    GPUCommunicator,
    TorchTensorAllocator,
)
from ray.experimental.channel.nccl_group import _NcclGroup

from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.tests.conftest import *  # noqa
from ray.experimental.util.types import ReduceOp

logger = logging.getLogger(__name__)

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))


@ray.remote
class TorchTensorWorker:
    def __init__(self):
        self.device = torch_utils.get_devices()[0]

    def init_distributed(self, world_size, rank):
        torch.distributed.init_process_group(
            backend="nccl", world_size=world_size, rank=rank
        )

    def send(self, shape, dtype, value: int, send_tensor=True):
        if not send_tensor:
            return 1
        return torch.ones(shape, dtype=dtype, device=self.device) * value

    def send_dict(self, entries):
        results = {}
        for key, entry in entries.items():
            value, shape, dtype = entry
            results[key] = torch.ones(shape, dtype=dtype) * value
        return results

    def send_or_raise(self, shape, dtype, value: int, raise_exception=False):
        if raise_exception:
            raise RuntimeError()
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
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_type_hint(TorchTensorType())
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
        assert ray.get(ref) == (i, shape, dtype)

    # Passing tensors of different sizes is okay.
    ref = compiled_dag.execute(i, shape=(20,), dtype=dtype)
    assert ray.get(ref) == (i, (20,), dtype)

    ref = compiled_dag.execute(i, shape=(5,), dtype=dtype)
    assert ray.get(ref) == (i, (5,), dtype)


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
        torch_inp = inp.with_type_hint(TorchTensorType())
        dag = receiver.recv.bind(torch_inp)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(torch.ones(shape, dtype=dtype) * i)
        result = ray.get(ref)
        assert result == (i, shape, dtype)

    # Passing tensors of different sizes is okay.
    ref = compiled_dag.execute(torch.ones((20,), dtype=dtype) * i)
    assert ray.get(ref) == (i, (20,), dtype)

    ref = compiled_dag.execute(torch.ones((5,), dtype=dtype) * i)
    assert ray.get(ref) == (i, (5,), dtype)


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

    # Test normal execution.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    # Test that we can pass different shapes and data.
    for i in range(3):
        shape = (10 * (i + 1),)
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
        assert ray.get(ref) == (i, shape, dtype)

    # Test that actors can be reused for a new DAG.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    # Test that we can pass different shapes and data.
    for i in range(3):
        shape = (10 * (i + 1),)
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
        assert ray.get(ref) == (i, shape, dtype)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_disallows_driver(ray_start_regular):
    """
    Check that the driver cannot participate in the NCCL group, i.e. DAG input
    and output nodes cannot have a TorchTensorType(transport="nccl")
    annotation.
    """
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

    # Test that InputNode cannot cannot participate in the NCCL group.
    with InputNode() as inp:
        torch_inp = inp.with_type_hint(TorchTensorType(transport="nccl"))
        dag = receiver.recv.bind(torch_inp)
    with pytest.raises(
        ValueError,
        match=(
            r"DAG inputs cannot be transferred "
            "via NCCL because the driver cannot participate in the NCCL group"
        ),
    ):
        dag.experimental_compile()

    # Test that OutputNode cannot cannot participate in the NCCL group.
    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))

    with pytest.raises(
        ValueError,
        match=(r"Driver cannot participate in the NCCL group\."),
    ):
        dag.experimental_compile()


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
            self._inner.allreduce(send_buf, recv_buf, op)
            recv_buf += 1

        def destroy(self) -> None:
            return self._inner.destroy()

    comm_id = nccl.get_unique_id()
    nccl_group = TestNcclGroup(2, comm_id, [sender, receiver])
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value)
        dag = dag.with_type_hint(TorchTensorType(transport=nccl_group))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        i += 1
        shape = (i * 10,)
        dtype = torch.float16
        kwargs = {
            "shape": shape,
            "dtype": dtype,
            "value": i,
        }
        ref = compiled_dag.execute(**kwargs)
        result = ray.get(ref)
        assert result == (i, shape, dtype)


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
        dag = actor1.send.bind(inp.shape, inp.dtype, inp.value)
        dag = dag.with_type_hint(TorchTensorType(transport=nccl_group))
        dag = actor2.recv.options(num_returns=3).bind(dag)
        dag = actor2.send.bind(*dag)
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = actor1.recv.bind(dag)
    with pytest.raises(
        ValueError,
        match=r"Accelerated DAGs do not support mixed usage of type hints.*",
    ):
        dag.experimental_compile()

    # Case 2: default NCCL group first, then custom NCCL group
    with InputNode() as inp:
        dag = actor1.send.bind(inp.shape, inp.dtype, inp.value)
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = actor2.recv.options(num_returns=3).bind(dag)
        dag = actor2.send.bind(*dag)
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
        dag = actor1.send.bind(inp.shape, inp.dtype, inp.value)
        dag = dag.with_type_hint(TorchTensorType(transport=nccl_group))
        dag = actor2.recv.options(num_returns=3).bind(dag)
        dag = actor2.send.bind(*dag)
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
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value)
        dag = dag.with_type_hint(TorchTensorType(transport=nccl_group))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        i += 1
        shape = (i * 10,)
        dtype = torch.float16
        kwargs = {
            "shape": shape,
            "dtype": dtype,
            "value": i,
        }
        ref = compiled_dag.execute(**kwargs)
        result = ray.get(ref)
        assert result == (i, shape, dtype)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_static_shape(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_type_hint(TorchTensorType(transport="nccl", _static_shape=True))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    # Test that the DAG works as long as we send the same shape.
    shape = (10,)
    dtype = torch.float16
    for i in range(3):
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
        assert ray.get(ref) == (i, shape, dtype)

    # Error is thrown if we send the wrong shape. Currently the receiver cannot
    # catch the exception so the DAG cannot be used again.
    with pytest.raises(RayChannelError):
        ref = compiled_dag.execute(i, shape=(20,), dtype=dtype)
        ray.get(ref)

    with pytest.raises(RayChannelError):
        ref = compiled_dag.execute(i, shape=(21,), dtype=dtype)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_direct_return(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value, inp.send_tensor)
        dag = dag.with_type_hint(TorchTensorType(transport="nccl", _direct_return=True))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        shape = (10 * (i + 1),)
        dtype = torch.float16
        ref = compiled_dag.execute(value=i, shape=shape, dtype=dtype, send_tensor=True)
        assert ray.get(ref) == (i, shape, dtype)

    # Error is thrown if we do not send a tensor. Currently the receiver cannot
    # catch the exception so the DAG cannot be used again.
    ref = compiled_dag.execute(value=i, shape=shape, dtype=dtype, send_tensor=False)
    with pytest.raises(RayChannelError):
        ray.get(ref)

    with pytest.raises(RayChannelError):
        ref = compiled_dag.execute(value=i, shape=shape, dtype=dtype, send_tensor=True)


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
        dag = sender.send_dict.bind(inp)
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = receiver.recv_dict.bind(dag)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        dtype = torch.float16
        args = {j: (j, (10 * j,), dtype) for j in range(1, i + 1)}

        ref = compiled_dag.execute(args)
        result = ray.get(ref)
        assert result == args


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
@pytest.mark.parametrize("static_shape", [False, True])
@pytest.mark.parametrize("direct_return", [False, True])
def test_torch_tensor_exceptions(ray_start_regular, static_shape, direct_return):
    """
    Test exceptions being thrown by a NCCL sending task.
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
        dag = dag.with_type_hint(
            TorchTensorType(
                _static_shape=static_shape,
                _direct_return=direct_return,
                transport="nccl",
            )
        )
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    shape = (10,)
    dtype = torch.float16

    for i in range(3):
        i += 1

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
    if static_shape or direct_return:
        with pytest.raises(RayChannelError):
            # TODO(swang): Ideally return the RuntimeError thrown by the
            # application instead of a generic RayChannelError.
            ray.get(ref)

        with pytest.raises(RayChannelError):
            # If using static_shape=True or direct_return=True, the DAG is not
            # usable after application-level exceptions.
            ref = compiled_dag.execute(
                shape=shape,
                dtype=dtype,
                value=i,
                raise_exception=False,
            )
    else:
        with pytest.raises(RuntimeError):
            ray.get(ref)

        # DAG should still be usable after application-level exceptions.
        ref = compiled_dag.execute(
            shape=shape,
            dtype=dtype,
            value=i,
            raise_exception=False,
        )
        result = ray.get(ref)
        assert result == (i, shape, dtype)


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
            self._inner.allreduce(send_buf, recv_buf, op)
            recv_buf += 1

        def destroy(self) -> None:
            return self._inner.destroy()

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
        # The custom communicator adds 1 to the tensor after the all-reduce.
        reduced_val += 1
        assert result == [(reduced_val, shape, dtype) for _ in workers]


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


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
