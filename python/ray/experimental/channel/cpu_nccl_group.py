import asyncio
from collections import defaultdict
from typing import Optional, Tuple, List
from unittest import mock

import torch

import ray
import ray.experimental.channel as ray_channel
from ray.experimental.channel.gpu_communicator import TorchTensorAllocator, ReduceOp


@ray.remote(num_cpus=0)
class Barrier:
    """
    Barrier that blocks the given number of actors until all actors have
    reached the barrier. This is used to mock out blocking NCCL ops.
    """

    def __init__(self, num_actors=2):
        self.num_actors = num_actors
        self.condition = asyncio.Condition()
        # Buffer for the data that is "sent" between the actors, each entry is
        # one p2p op.
        self.data = {}
        self.collective_data = defaultdict(list)
        # Buffer for the number of actors seen, each entry is one p2p op.
        self.num_actors_seen = defaultdict(int)

    async def wait(self, op_id: int, data=None):
        """
        Wait at barrier until all actors have sent `op_id`. One actor should
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

        return data

    async def wait_collective(self, op_id: int, data: torch.Tensor, op: ReduceOp):
        """
        Wait at the barrier until all actors have sent `op_id` and `data`.
        Once data from all actors is received, execute the collective `op`
        on the barrier actor and return the result.
        """
        async with self.condition:
            if op_id not in self.collective_data:
                self.collective_data[op_id].append(data)
            self.num_actors_seen[op_id] += 1

            if self.num_actors_seen[op_id] == self.num_actors:
                # Apply the collective operation across all gathered tensors
                result = self._apply_op(op, self.collective_data[op_id])
                self.collective_data[op_id] = result
                self.condition.notify_all()
            else:
                await self.condition.wait_for(lambda: self.num_actors_seen[op_id] == self.num_actors)

            # Return the result to all actors
            return self.collective_data[op_id]

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
            # reserve a place for future ops to be added
            assert False, "current operation not supported"
        return result

class CPUNcclGroup(ray_channel.nccl_group._NcclGroup):
    """
    Mock the internal _NcclGroup to use a barrier actor instead of a NCCL group
    for communication.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # We use the op index to synchronize the sender and receiver at the
        # barrier.
        self.num_ops = defaultdict(int)
        self.barriers = set()

    def send(self, tensor: torch.Tensor, peer_rank: int):
        # "Send" the tensor to the barrier actor.
        barrier_key = f"barrier-{self.get_self_rank()}-{peer_rank}"
        barrier = ray.get_actor(name=barrier_key)
        self.barriers.add(barrier)
        ray.get(barrier.wait.remote(self.num_ops[barrier_key], tensor))
        self.num_ops[barrier_key] += 1

    def recv(
        self,
        shape: Tuple[int],
        dtype: torch.dtype,
        peer_rank: int,
        allocator: Optional[TorchTensorAllocator] = None,
    ):
        # "Receive" the tensor from the barrier actor.
        barrier_key = f"barrier-{peer_rank}-{self.get_self_rank()}"
        barrier = ray.get_actor(name=barrier_key)
        self.barriers.add(barrier)
        received_tensor = ray.get(barrier.wait.remote(self.num_ops[barrier_key]))
        assert (
            allocator is not None
        ), "torch tensor allocator is required for CPUNcclGroup"
        buf = allocator(shape, dtype)
        buf[:] = received_tensor[:]
        self.num_ops[barrier_key] += 1
        return buf

    def allreduce(self, send_buf: torch.Tensor, recv_buf: torch.Tensor, op: ReduceOp = ReduceOp.SUM):
        # different collective communications can use same barrier as long as the participants are the same
        barrier_key = "barrier-"+"-".join(map(str, sorted(peer_rank + [self.get_self_rank()])))
        barrier = ray.get_actor(name=barrier_key)
        self.barriers.add(barrier)

        result = ray.get(barrier.wait_collective.remote(self.num_ops[barrier_key], send_buf, op))

        assert (
            recv_buf is not None
        ), "Receiving buffer required for CPUNcclGroup"
        recv_buf[:] = result[:]
        self.num_ops[barrier_key] += 1

    def destroy(self) -> None:
        for barrier in self.barriers:
            ray.kill(barrier)

