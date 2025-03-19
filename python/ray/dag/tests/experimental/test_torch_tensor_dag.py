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
import time
from ray.dag import InputNode
from ray.exceptions import RayChannelError, RayTaskError
from ray.dag.output_node import MultiOutputNode
from ray.experimental.channel.communicator import (
    Communicator,
    TorchTensorAllocator,
)
from ray.experimental.channel.utils import get_default_torch_device
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.experimental.channel.nccl_group import _NcclGroup
from ray._private.test_utils import (
    get_log_message,
    init_log_pubsub,
)

from ray.tests.conftest import *  # noqa
from ray.experimental.util.types import ReduceOp

logger = logging.getLogger(__name__)

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))


@ray.remote
class TorchTensorWorker:
    def __init__(self):
        self.device = get_default_torch_device(allow_cpu=True)

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

    def send_int(self, value: int):
        return value

    def recv(self, tensor):
        return (tensor[0].item(), tensor.shape, tensor.dtype)

    def recv_and_matmul(self, two_d_tensor):
        """
        Receive the tensor and do some expensive computation (matmul).

        Args:
            two_d_tensor: a 2D tensor that has the same size for its dimensions
        """
        # Check that tensor got loaded to the correct device.
        assert two_d_tensor.dim() == 2
        assert two_d_tensor.size(0) == two_d_tensor.size(1)
        torch.matmul(two_d_tensor, two_d_tensor)
        return (two_d_tensor[0][0].item(), two_d_tensor.shape, two_d_tensor.dtype)

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
        return tensor

    def ping(self):
        return

    @ray.method(num_returns=2)
    def return_two_tensors(
        self, t1: torch.Tensor, t2: torch.Tensor
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        return t1, t2


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
        dag = dag.with_tensor_transport()
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
        torch_inp = inp.with_tensor_transport()
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
@pytest.mark.parametrize("enable_profiling", [False, True])
@pytest.mark.parametrize("overlap_gpu_communication", [False, True])
def test_torch_tensor_nccl(
    ray_start_regular, monkeypatch, enable_profiling, overlap_gpu_communication
):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    monkeypatch.setattr(
        ray.dag.constants, "RAY_CGRAPH_ENABLE_PROFILING", enable_profiling
    )

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    # Test normal execution.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_tensor_transport(transport="nccl")
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile(
        _overlap_gpu_communication=overlap_gpu_communication,
    )

    # Test that we can pass different shapes and data.
    for i in range(3):
        shape = (10 * (i + 1),)
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
        assert ray.get(ref) == (i, shape, dtype)

    # We need to explicitly teardown because Python will not del right
    # when the reassign happens, so channels will not be properly closed
    # before we open new ones on these actors below.
    compiled_dag.teardown()

    # Test that actors can be reused for a new DAG.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_tensor_transport(transport="nccl")
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    # Test that we can pass different shapes and data.
    for i in range(3):
        shape = (10 * (i + 1),)
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
        assert ray.get(ref) == (i, shape, dtype)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
@pytest.mark.parametrize("num_gpus", [[0, 0], [1, 0], [0, 1], [1, 1], [0.5, 0.5]])
def test_torch_tensor_auto(ray_start_regular, num_gpus):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    sender = TorchTensorWorker.options(num_cpus=0, num_gpus=num_gpus[0]).remote()
    receiver = TorchTensorWorker.options(num_cpus=0, num_gpus=num_gpus[1]).remote()
    # Use NCCL only when sender and receiver are on different GPUs.
    # When each actor has 0.5 GPU, sender and receiver are allocated
    # on the same GPU, so we use auto.
    expected_transport = "nccl" if num_gpus == [1, 1] else "auto"

    shape = (10,)
    dtype = torch.float16
    device = "cpu" if num_gpus[0] == 0 or num_gpus[1] == 0 else "default"

    # Test normal execution.
    with InputNode() as inp:
        data = sender.send.bind(inp.shape, inp.dtype, inp[0])
        data_annotated = data.with_tensor_transport(transport="auto", device=device)
        dag = receiver.recv.bind(data_annotated)

    compiled_dag = dag.experimental_compile()
    assert isinstance(data_annotated.type_hint, TorchTensorType)
    assert data_annotated.type_hint.transport == expected_transport

    # Test that we can pass different shapes and data.
    for i in range(3):
        shape = (10 * (i + 1),)
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
        assert ray.get(ref) == (i, shape, dtype)

    # We need to explicitly teardown because Python will not del right
    # when the reassign happens, so channels will not be properly closed
    # before we open new ones on these actors below.
    compiled_dag.teardown()

    # Test that actors can be reused for a new DAG.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_tensor_transport(transport="auto", device=device)
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    assert isinstance(data_annotated.type_hint, TorchTensorType)
    assert data_annotated.type_hint.transport == expected_transport

    # Test that we can pass different shapes and data.
    for i in range(3):
        shape = (10 * (i + 1),)
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
        assert ray.get(ref) == (i, shape, dtype)


@pytest.mark.parametrize(
    "ray_start_regular, overlap_gpu_communication",
    [({"num_cpus": 4}, False), ({"num_cpus": 4}, True)],
    indirect=["ray_start_regular"],
)
def test_torch_tensor_nccl_overlap_timed(ray_start_regular, overlap_gpu_communication):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) >= 4
    ), "This test requires at least 4 GPUs"

    worker_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)
    num_senders = 3
    senders = [worker_cls.remote() for _ in range(num_senders)]
    receiver = worker_cls.remote()

    shape = (10000, 10000)
    dtype = torch.float16

    with InputNode() as inp:
        branches = [sender.send.bind(shape, dtype, inp) for sender in senders]
        branches = [
            branch.with_tensor_transport(
                transport="nccl", _static_shape=True, _direct_return=True
            )
            for branch in branches
        ]
        branches = [receiver.recv_and_matmul.bind(branch) for branch in branches]
        dag = MultiOutputNode(branches)

    # Test normal execution.
    compiled_dag = dag.experimental_compile(
        _overlap_gpu_communication=overlap_gpu_communication,
    )

    start = time.monotonic()
    for i in range(5):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        assert result == [(i, shape, dtype)] * num_senders
    duration = time.monotonic() - start
    print(f"{overlap_gpu_communication=}, {duration=}")

    compiled_dag.teardown()


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
        torch_inp = inp.with_tensor_transport(transport="nccl")
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
        dag = dag.with_tensor_transport(transport="nccl")

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

    class TestNcclGroup(Communicator):
        """
        A custom NCCL group for testing. This is a simple wrapper around `_NcclGroup`.
        """

        import cupy as cp

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

        @property
        def recv_stream(self) -> Optional["cp.cuda.ExternalStream"]:
            return self._inner.recv_stream

        @property
        def send_stream(self) -> Optional["cp.cuda.ExternalStream"]:
            return self._inner.send_stream

        def destroy(self) -> None:
            return self._inner.destroy()

        def get_transport_name(self) -> str:
            return "nccl"

    from cupy.cuda import nccl

    comm_id = nccl.get_unique_id()
    nccl_group = TestNcclGroup(2, comm_id, [sender, receiver])
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value)
        dag = dag.with_tensor_transport(transport=nccl_group)
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

    class InitedNcclGroup(Communicator):
        """
        A custom NCCL group based on existing torch.distributed setup.
        """

        import cupy as cp

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
            self._device = get_default_torch_device(allow_cpu=True)

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

        @property
        def recv_stream(self) -> Optional["cp.cuda.ExternalStream"]:
            import cupy as cp

            return cp.cuda.get_current_stream()

        @property
        def send_stream(self) -> Optional["cp.cuda.ExternalStream"]:
            import cupy as cp

            return cp.cuda.get_current_stream()

        def destroy(self) -> None:
            pass

        def get_transport_name(self) -> str:
            return "nccl"

    nccl_group = InitedNcclGroup(2, [sender, receiver])

    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value)
        dag = dag.with_tensor_transport(transport=nccl_group)
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
@pytest.mark.parametrize(
    "transports",
    [["auto", "nccl"], ["custom", "nccl"], ["auto", "nccl"], ["custom", "custom"]],
)
def test_torch_tensor_default_comm(ray_start_regular, transports):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 2
    ), "This test requires at least 3 GPUs"
    runtime_env = {
        "env_vars": {
            "MASTER_ADDR": socket.gethostbyname(socket.gethostname()),
            "MASTER_PORT": "8888",
        }
    }
    actor_cls = TorchTensorWorker.options(
        num_cpus=0, num_gpus=1, runtime_env=runtime_env
    )

    worker0 = actor_cls.remote()
    worker1 = actor_cls.remote()
    worker2 = actor_cls.remote()

    # Simulates that the distributed environment (e.g., torch.distributed)
    # have already been set up
    refs = [
        worker0.init_distributed.remote(3, 0),
        worker1.init_distributed.remote(3, 1),
        worker2.init_distributed.remote(3, 2),
    ]
    ray.wait(refs)

    class InitedNcclGroup(Communicator):
        """
        A custom NCCL group based on existing torch.distributed setup.
        """

        import cupy as cp

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
            self._device = get_default_torch_device(allow_cpu=True)

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

        @property
        def recv_stream(self) -> Optional["cp.cuda.ExternalStream"]:
            import cupy as cp

            return cp.cuda.get_current_stream()

        @property
        def send_stream(self) -> Optional["cp.cuda.ExternalStream"]:
            import cupy as cp

            return cp.cuda.get_current_stream()

        def destroy(self) -> None:
            pass

        def get_transport_name(self) -> str:
            return "nccl"

    default_comm = InitedNcclGroup(3, [worker0, worker1, worker2])
    custom_comm = InitedNcclGroup(3, [worker0, worker1, worker2])
    custom_comm_count = 0
    for i in range(2):
        if transports[i] == "custom":
            transports[i] = custom_comm
            custom_comm_count += 1

    with InputNode() as inp:
        dag = worker0.send.bind(inp.shape, inp.dtype, inp.value)
        dag = dag.with_tensor_transport(transport=transports[0])
        dag = worker1.recv_tensor.bind(dag)
        dag = dag.with_tensor_transport(transport=transports[1])
        dag = worker2.recv.bind(dag)

    compiled_dag = dag.experimental_compile(_default_communicator=default_comm)
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

    # No communicators are created, the default communicator is used
    assert len(compiled_dag._actors_to_created_communicator_id) == 0
    assert compiled_dag._default_communicator == default_comm
    if custom_comm_count == 0:
        assert len(compiled_dag._communicator_to_type_hints) == 1
    elif custom_comm_count == 1:
        assert len(compiled_dag._communicator_to_type_hints) == 2
    else:
        assert custom_comm_count == 2
        assert len(compiled_dag._communicator_to_type_hints) == 1


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_invalid_custom_comm(ray_start_regular):
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

    class UserCreatedNcclGroup(Communicator):
        """
        A custom NCCL group based on existing torch.distributed setup.
        """

        import cupy as cp

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
            self._device = get_default_torch_device(allow_cpu=True)

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

        @property
        def recv_stream(self) -> Optional["cp.cuda.ExternalStream"]:
            import cupy as cp

            return cp.cuda.get_current_stream()

        @property
        def send_stream(self) -> Optional["cp.cuda.ExternalStream"]:
            import cupy as cp

            return cp.cuda.get_current_stream()

        def destroy(self) -> None:
            pass

        def get_transport_name(self) -> str:
            return "nccl"

    comm2 = UserCreatedNcclGroup(2, [sender, receiver])
    comm1 = UserCreatedNcclGroup(1, [sender])

    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value)
        dag = dag.with_tensor_transport(transport="auto")
        dag = receiver.recv.bind(dag)

    with pytest.raises(
        ValueError,
        match="The passed-in communicator must include all of the actors used in the P2P operation.",
    ):
        dag.experimental_compile(_default_communicator=comm1)

    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value)
        dag = dag.with_tensor_transport(transport=comm1)
        dag = receiver.recv.bind(dag)

    with pytest.raises(
        ValueError,
        match="The passed-in communicator must include all of the actors used in the P2P operation.",
    ):
        dag.experimental_compile(_default_communicator=comm2)


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
        dag = dag.with_tensor_transport(transport="nccl", _static_shape=True)
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
    with pytest.raises(RayTaskError):
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
        dag = dag.with_tensor_transport(transport="nccl", _direct_return=True)
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
    with pytest.raises(RayTaskError):
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
        dag = dag.with_tensor_transport(transport="nccl")
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
@pytest.mark.parametrize("overlap_gpu_communication", [False, True])
def test_torch_tensor_exceptions(
    ray_start_regular, static_shape, direct_return, overlap_gpu_communication
):
    """
    Test exceptions being thrown by a NCCL sending task's execution.
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
        dag = dag.with_tensor_transport(
            transport="nccl",
            _static_shape=static_shape,
            _direct_return=direct_return,
        )
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile(
        _overlap_gpu_communication=overlap_gpu_communication,
    )

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
        with pytest.raises(RuntimeError):
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
def test_torch_tensor_exceptions2(
    ray_start_regular,
):
    """
    Test exceptions being thrown by a NCCL sending task's write operation.
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
        dag = sender.send_int.bind(inp)
        dag = dag.with_tensor_transport(
            transport="nccl",
            _direct_return=True,
            _static_shape=True,
        )
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute(1)
    with pytest.raises(
        ValueError,
        match=(
            "Task annotated with _direct_return=True must return a "
            "CUDA torch.Tensor, instead found value `1`. "
            "DAG will shut down."
        ),
    ):
        ray.get(ref)

    with pytest.raises(RayChannelError):
        # The DAG is not usable after the exception.
        ref = compiled_dag.execute(2)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_explicit_communicator(ray_start_regular):
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
        dag = dag.with_tensor_transport(transport="nccl")
        dag = receiver.recv.bind(dag)

    with pytest.raises(
        ValueError,
        match=(
            "Please specify a custom communicator for the DAGNode using "
            r"`with_tensor_transport\(\)`, or specify a communicator or 'create' for "
            "_default_communicator when calling experimental_compile()."
        ),
    ):
        dag.experimental_compile(_default_communicator=None)

    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_tensor_transport(transport="auto")
        dag = receiver.recv.bind(dag)

    with pytest.raises(
        ValueError,
        match=(
            "This requires specifying a default communicator or 'create' for "
            "_default_communicator when calling experimental_compile()."
        ),
    ):
        dag.experimental_compile(_default_communicator=None)


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
        dag = MultiOutputNode([recv, tensor, collectives[1]])

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        ref = compiled_dag.execute(
            [(shape, dtype, i + idx + 1) for idx in range(num_workers)]
        )
        result = ray.get(ref)
        metadata, tensor, _ = result
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

    # Since we have buffered channels, the execution should not error, but the
    # get should error, as the dag should no longer work after the application-
    # level exception.
    ref = compiled_dag.execute([((20,), dtype, 1) for _ in workers])
    with pytest.raises(RayChannelError):
        ray.get(ref)


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

    class TestNcclGroup(Communicator):
        """
        A custom NCCL group for testing. This is a simple wrapper around `_NcclGroup`.
        """

        import cupy as cp

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

        @property
        def recv_stream(self) -> Optional["cp.cuda.ExternalStream"]:
            return self._inner.recv_stream

        @property
        def send_stream(self) -> Optional["cp.cuda.ExternalStream"]:
            return self._inner.send_stream

        def destroy(self) -> None:
            return self._inner.destroy()

        def get_transport_name(self) -> str:
            return "nccl"

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

        # Tensor to be sent from workers[0] to workers[1].
        t = workers[0].send.bind(shape, dtype, inp)
        t.with_tensor_transport(transport="nccl")

        collectives = collective.allreduce.bind([x, y])
        recv = workers[1].recv.bind(t)
        dag = MultiOutputNode([collectives[0], collectives[1], recv])

    compiled_dag = dag.experimental_compile()

    value = 10
    ref = compiled_dag.execute(value)
    result = ray.get(ref)
    reduced_value = value * 2
    expected_tensor_val = torch.ones(shape, dtype=dtype) * reduced_value
    assert torch.equal(result[0].cpu(), expected_tensor_val)
    assert torch.equal(result[1].cpu(), expected_tensor_val)
    assert result[2] == (value, shape, dtype)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_nccl_all_reduce_with_class_method_output_node(ray_start_regular):
    """
    Test all-reduce with class method output node.
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
        t1, t2 = workers[0].return_two_tensors.bind(inp[0], inp[1])
        t3, t4 = workers[1].return_two_tensors.bind(inp[2], inp[3])
        tensors = collective.allreduce.bind([t1, t4], ReduceOp.SUM)
        dag = MultiOutputNode(tensors + [t2, t3])

    compiled_dag = dag.experimental_compile()

    t1 = torch.tensor([1], device="cuda")
    t2 = torch.tensor([2], device="cuda")
    t3 = torch.tensor([3], device="cuda")
    t4 = torch.tensor([4], device="cuda")

    for i in range(3):
        i += 1
        ref = compiled_dag.execute(t1, t2, t3, t4)
        result = ray.get(ref)
        assert result == [t1 + t4, t1 + t4, t2, t3]


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
def test_tensor_writable_warning_suppressed(ray_start_regular):
    """When we move cpu tensor to gpu, Compiled Graph does zero-copy with is_writable=False.
    Torch doesn't like it, so it prints warning. We know that it is safe to do it,
    so Ray suppress the warning message. This test verifies the warning is not
    printed in this scenario.

    """
    if not USE_GPU:
        pytest.skip("Test requires GPU")

    p = init_log_pubsub()

    @ray.remote(num_gpus=1)
    class A:
        def recv(self, tensor):
            return 1

    receiver = A.remote()

    # Test torch.Tensor as input.
    with InputNode() as inp:
        # TODO(swang): Test that we are using the minimum number of
        # channels/messages when _direct_return=True.
        torch_inp = inp.with_tensor_transport()
        dag = receiver.recv.bind(torch_inp)

    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(torch.tensor([1]))
    assert ray.get(ref) == 1
    # This should timeout because actor shouldn't print anything.
    logs = get_log_message(p, 2, timeout=3)
    # Verify nothing else is published other than autoscaler messages.
    # If warning is not suppressed, warning should be printed here.
    for log in logs:
        assert "The given NumPy array is not writable" not in log, log
    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_nccl_channel_with_local_reader(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    w1 = actor_cls.remote()
    w2 = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = w1.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_tensor_transport(transport="nccl")
        branch1 = w1.recv.bind(dag)
        branch2 = w2.recv.bind(dag)
        dag = MultiOutputNode([branch1, branch2])
    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
        assert ray.get(ref) == [(i, shape, dtype), (i, shape, dtype)]

    # Passing tensors of different sizes is okay.
    ref = compiled_dag.execute(i, shape=(20,), dtype=dtype)
    assert ray.get(ref) == [(i, (20,), dtype), (i, (20,), dtype)]

    ref = compiled_dag.execute(i, shape=(5,), dtype=dtype)
    assert ray.get(ref) == [(i, (5,), dtype), (i, (5,), dtype)]


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_nccl_channel_with_two_local_readers(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")
    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"
    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    w1 = actor_cls.remote()
    w2 = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = w1.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_tensor_transport(transport="nccl")
        branch1 = w1.recv.bind(dag)
        branch2 = w1.recv.bind(dag)
        branch3 = w2.recv.bind(dag)
        dag = MultiOutputNode([branch1, branch2, branch3])
    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
        assert ray.get(ref) == [(i, shape, dtype), (i, shape, dtype), (i, shape, dtype)]

    # Passing tensors of different sizes is okay.
    ref = compiled_dag.execute(i, shape=(20,), dtype=dtype)
    assert ray.get(ref) == [(i, (20,), dtype), (i, (20,), dtype), (i, (20,), dtype)]

    ref = compiled_dag.execute(i, shape=(5,), dtype=dtype)
    assert ray.get(ref) == [(i, (5,), dtype), (i, (5,), dtype), (i, (5,), dtype)]


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_nccl_channel_with_all_local_readers(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")
    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 0
    ), "This test requires at least 1 GPU"
    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    worker = actor_cls.remote()

    with InputNode() as inp:
        dag = worker.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_tensor_transport(transport="nccl")
        dag = MultiOutputNode([worker.recv.bind(dag)])
    with pytest.raises(
        AssertionError,
        match=(
            "All readers are from the same actor. The TorchTensorType type hint "
            "is not needed. No NCCL channel will be created."
        ),
    ):
        dag.experimental_compile()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
