import asyncio
from collections import defaultdict
from typing import Optional, Tuple, List, Dict
from unittest import mock

import torch

import ray
import ray.experimental.channel as ray_channel
from ray.experimental.channel.gpu_communicator import TorchTensorAllocator, ReduceOp

@ray.remote(num_cpus=0)
class CPUCommunicator:
    """
    Communicator actor that blocks the given number of actors until all actors have
    reached the communicator. This is used to mock out blocking NCCL ops.
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

            if self.num_actors_seen[op_id] == self.num_actors:
                del self.data[op_id]
                del self.num_actors_seen[op_id]

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
                result = self._apply_op(op, self.collective_data[op_id])
                self.collective_data[op_id] = result
                self.condition.notify_all()
            else:
                await self.condition.wait_for(lambda: self.num_actors_seen[op_id] == self.num_actors)

            result = self.collective_data[op_id]

            if self.num_actors_seen[op_id] == self.num_actors:
                del self.collective_data[op_id]
                del self.num_actors_seen[op_id]

            return result

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


class CPUNcclGroup(ray_channel.nccl_group._NcclGroup):
    """
    Mock the internal _NcclGroup to use a communicator actor instead of a NCCL group
    for communication.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        """We use the op index to synchronize the sender and receiver at the
        communicator actor."""
        self.num_ops = defaultdict(int)
        self.communicators = set()

    def _ensure_communicator_exists(self, comm_key: str, is_p2p: bool = False):
        """Ensure the necessary communicator actor is created."""
        try:
            ray.get_actor(name=comm_key)
        except ValueError:
            ray.remote(CPUCommunicator).options(name=comm_key).remote(2 if is_p2p else self._world_size)

    def send(self, tensor: torch.Tensor, peer_rank: int):
        """Send the tensor to the communicator actor."""
        comm_key = f"communicator-{self.get_self_rank()}-{peer_rank}"
        self._ensure_communicator_exists(comm_key, is_p2p=True)
        communicator = ray.get_actor(name=comm_key)

        self.communicators.add(communicator)
        ray.get(communicator.wait_p2p.remote(self.num_ops[comm_key], tensor))
        self.num_ops[comm_key] += 1

    def recv(
        self,
        shape: Tuple[int],
        dtype: torch.dtype,
        peer_rank: int,
        allocator: Optional[TorchTensorAllocator] = None,
    ):
        """Receive the tensor from the communicator actor."""
        comm_key = f"communicator-{peer_rank}-{self.get_self_rank()}"
        self._ensure_communicator_exists(comm_key, is_p2p=True)
        communicator = ray.get_actor(name=comm_key)
        self.communicators.add(communicator)
        received_tensor = ray.get(communicator.wait_p2p.remote(self.num_ops[comm_key]))

        assert allocator is not None, "torch tensor allocator is required for CPUNcclGroup"
        buf = allocator(shape, dtype)
        buf[:] = received_tensor[:]
        self.num_ops[comm_key] += 1
        return buf

    def allreduce(self, send_buf: torch.Tensor, recv_buf: torch.Tensor, op: ReduceOp = ReduceOp.SUM):
        comm_key = f"communicator_{id(self)}"
        self._ensure_communicator_exists(comm_key)
        comm = ray.get_actor(name=comm_key)
        self.communicators.add(comm)

        result = ray.get(comm.wait_collective.remote(self.num_ops[comm_key], send_buf, op))

        assert recv_buf is not None, "Receiving buffer required for CPUNcclGroup"
        recv_buf[:] = result[:]
        self.num_ops[comm_key] += 1

    def destroy(self) -> None:
        for communicator in self.communicators:
            ray.kill(communicator)