import asyncio
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
from unittest import mock

import torch

import ray
import ray.experimental.channel as ray_channel
from ray.experimental.channel.gpu_communicator import (
    GPUCommunicator,
    ReduceOp,
    TorchTensorAllocator,
)


@ray.remote(num_cpus=0)
class CPUCommunicator:
    """
    Communicator actor that blocks the given number of actors until all actors have
    reached the communicator. This is used to mock out blocking NCCL ops.

    A CPUCommunicator which is used for collective ops will never be used for p2p ops,
    and vice versa.
    """

    def __init__(self, num_actors=2):
        self.num_actors = num_actors
        self.condition = asyncio.Condition()
        # Buffer for the data that is "sent" between the actors, each entry is
        # one p2p op.
        self.data: Dict[int, torch.Tensor] = {}
        self.collective_data: Dict[int, List[torch.Tensor]] = defaultdict(list)
        # Buffer for the number of actors seen, each entry is one p2p op.
        self.num_actors_seen = defaultdict(int)
        # Number of actors who have read the result, and are about the exit the function.
        # State is kept so we only garbage collect after the last actor has read the
        # relevant data.
        self.num_actors_read = defaultdict(int)

    async def wait_p2p(self, op_id: int, data=None):
        """
        Wait at communicator until all actors have sent `op_id`. One actor should
        provide `data`, and this value will be returned by this method for all
        other actors.
        """
        async with self.condition:
            if data is not None:
                assert op_id not in self.data, (self.data, self.num_actors_seen)
                self.data[op_id] = data
            self.num_actors_seen[op_id] += 1

            if self.num_actors_seen[op_id] == self.num_actors:
                # Wake up all tasks waiting on this condition.
                self.condition.notify_all()
            else:
                await self.condition.wait_for(
                    lambda: self.num_actors_seen[op_id] == self.num_actors
                )

            if data is None:
                data = self.data[op_id]
            self.num_actors_read[op_id] += 1

            if self.num_actors_read[op_id] == self.num_actors:
                del self.data[op_id]
                del self.num_actors_seen[op_id]
                del self.num_actors_read[op_id]

            return data

    async def wait_collective(self, op_id: int, data: torch.Tensor, op: ReduceOp):
        """
        Wait at the communicator until all actors have sent `op_id` and `data`.
        Once data from all actors is received, execute the collective `op`
        on the communicator actor and return the result.
        """
        async with self.condition:
            self.collective_data[op_id].append(data)
            self.num_actors_seen[op_id] += 1

            if self.num_actors_seen[op_id] == self.num_actors:
                # Apply the collective operation across all gathered tensors
                data = self._apply_op(op, self.collective_data[op_id])
                self.collective_data[op_id] = data
                self.condition.notify_all()
            else:
                await self.condition.wait_for(
                    lambda: self.num_actors_seen[op_id] == self.num_actors
                )

            data = self.collective_data[op_id]
            self.num_actors_read[op_id] += 1

            if self.num_actors_read[op_id] == self.num_actors:
                del self.collective_data[op_id]
                del self.num_actors_seen[op_id]
                del self.num_actors_read[op_id]

            return data

    def _apply_op(self, op: ReduceOp, tensors: List[torch.Tensor]) -> torch.Tensor:
        """Apply the specified reduction operation across a list of tensors."""
        result = tensors[0].clone()
        if op == ReduceOp.SUM:
            for tensor in tensors[1:]:
                result += tensor
        elif op == ReduceOp.PRODUCT:
            for tensor in tensors[1:]:
                result *= tensor
        elif op == ReduceOp.MAX:
            for tensor in tensors[1:]:
                result = torch.max(result, tensor)
        elif op == ReduceOp.MIN:
            for tensor in tensors[1:]:
                result = torch.min(result, tensor)
        elif op == ReduceOp.AVG:
            result = sum(tensors) / len(tensors)
        else:
            raise ValueError(f"Operation {op} not supported")
        return result


class CPUNcclGroup(GPUCommunicator):
    """
    Mock the internal _NcclGroup to use a communicator actor instead of a NCCL group
    for communication.
    """

    def __init__(self, world_size: int, actor_handles: List["ray.actor.ActorHandle"]):
        """We use the op index to synchronize the sender and receiver at the
        communicator actor."""
        self._world_size = world_size
        self._actor_handles = actor_handles
        self.num_ops = defaultdict(int)
        self.communicators = set()
        self._rank = None

    def send(self, tensor: torch.Tensor, peer_rank: int):
        """Send the tensor to the communicator actor."""
        comm_key = f"communicator-p2p-{self.get_self_rank()}-{peer_rank}"
        comm = CPUCommunicator.options(name=comm_key, get_if_exists=True).remote(2)
        self.communicators.add(comm)

        ray.get(comm.wait_p2p.remote(self.num_ops[comm_key], tensor))
        self.num_ops[comm_key] += 1

    def recv(
        self,
        shape: Tuple[int],
        dtype: torch.dtype,
        peer_rank: int,
        allocator: Optional[TorchTensorAllocator] = None,
    ):
        """Receive the tensor from the communicator actor."""
        comm_key = f"communicator-p2p-{peer_rank}-{self.get_self_rank()}"
        comm = CPUCommunicator.options(name=comm_key, get_if_exists=True).remote(2)
        self.communicators.add(comm)

        received_tensor = ray.get(comm.wait_p2p.remote(self.num_ops[comm_key]))
        assert (
            allocator is not None
        ), "torch tensor allocator is required for CPUNcclGroup"
        buf = allocator(shape, dtype)
        buf[:] = received_tensor[:]
        self.num_ops[comm_key] += 1
        return buf

    def allreduce(
        self,
        send_buf: torch.Tensor,
        recv_buf: torch.Tensor,
        op: ReduceOp = ReduceOp.SUM,
    ):
        all_ranks = [
            self.get_rank(actor_handle) for actor_handle in self.get_actor_handles()
        ]
        comm_key = "communicator-collective-" + "-".join(map(str, sorted(all_ranks)))
        comm = CPUCommunicator.options(name=comm_key, get_if_exists=True).remote(
            self._world_size
        )
        self.communicators.add(comm)

        result = ray.get(
            comm.wait_collective.remote(self.num_ops[comm_key], send_buf, op)
        )
        assert recv_buf is not None, "Receiving buffer required for CPUNcclGroup"
        recv_buf[:] = result[:]
        self.num_ops[comm_key] += 1

    def destroy(self) -> None:
        for communicator in self.communicators:
            ray.kill(communicator)

    def initialize(self, rank: int) -> None:
        self._rank = rank

    def get_actor_handles(self) -> List["ray.actor.ActorHandle"]:
        return self._actor_handles

    def get_rank(self, actor: ray.actor.ActorHandle) -> int:
        """
        Return the given actor's rank in the NCCL communicator.

        Args:
            actor: The actor handle to look up.
        """
        actor_ids = [a._ray_actor_id for a in self._actor_handles]
        try:
            rank = actor_ids.index(actor._ray_actor_id)
        except ValueError:
            raise ValueError("Actor is not in the NCCL group.")
        return rank

    def get_self_rank(self) -> Optional[int]:
        return self._rank

    def get_world_size(self) -> int:
        """
        Return the number of ranks in the NCCL communicator.
        """
        return self._world_size

    def recv_stream(self):
        raise NotImplementedError

    def send_stream(self):
        raise NotImplementedError


class MockCudaStream:
    def __init__(self):
        self.cuda_stream = 0


def start_nccl_mock():
    """
    Patch methods that require CUDA.
    """
    # Mock cupy dependencies.
    nccl_mock = mock.MagicMock()
    nccl_mock.nccl.get_unique_id.return_value = 0
    cp_patcher = mock.patch.dict(
        "sys.modules",
        {
            "cupy.cuda": nccl_mock,
            "cupy": mock.MagicMock(),
            "ray.util.collective.collective_group": mock.MagicMock(),
        },
    )
    cp_patcher.start()

    # PyTorch mocks.
    stream_patcher = mock.patch(
        "torch.cuda.current_stream", new_callable=lambda: MockCudaStream
    )
    stream_patcher.start()
    new_stream_patcher = mock.patch(
        "torch.cuda.Stream", new_callable=lambda: MockCudaStream
    )
    new_stream_patcher.start()
    tensor_patcher = mock.patch("torch.Tensor.device", torch.device("cuda"))
    tensor_patcher.start()
    tensor_patcher = mock.patch("torch.Tensor.is_cuda", True)
    tensor_patcher.start()
    tensor_allocator_patcher = mock.patch(
        "ray.experimental.channel.torch_tensor_nccl_channel._torch_zeros_allocator",
        lambda shape, dtype: torch.zeros(shape, dtype=dtype),
    )
    tensor_allocator_patcher.start()

    ctx = ray_channel.ChannelContext.get_current()
    ctx.set_torch_device(torch.device("cuda"))
