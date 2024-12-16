import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import ray
from ray.experimental.channel.communicator import (
    Communicator,
    ReduceOp,
    TorchTensorAllocator,
)

if TYPE_CHECKING:
    import torch


@ray.remote(num_cpus=0)
class CPUCommBarrier:
    """
    Barrier actor that blocks the given number of actors until all actors have
    reached the Barrier.

    p2p operations are not done here (completed via shared memory channel).
    """

    def __init__(self, num_actors: int):
        self.num_actors = num_actors
        self.condition = asyncio.Condition()
        # Stores the data for each collective operation
        self.collective_data: Dict[int, List["torch.Tensor"]] = defaultdict(list)
        # Stores the shape of data for each collective operation
        self.collective_data_shape: Dict[int, "torch.Tensor.type"] = {}
        # Buffer for the number of actors seen
        self.num_actors_seen = defaultdict(int)
        # Number of actors who have read the result, and are about to exit the function.
        # State is kept so we only garbage collect after the last actor has read the
        # relevant data.
        self.num_actors_read = defaultdict(int)

    async def wait_collective(self, op_id: int, data: "torch.Tensor", op: ReduceOp):
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

    def _apply_op(self, op: ReduceOp, tensors: List["torch.Tensor"]) -> "torch.Tensor":
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


class CPUCommunicator(Communicator):
    """
    Uses a CPU-based communicator actor instead of a NCCL group.
    """

    def __init__(self, world_size: int, actor_handles: List["ray.actor.ActorHandle"]):
        """We use the op index to synchronize the sender and receiver at the
        communicator actor."""
        self._world_size = world_size
        self._actor_handles = actor_handles
        self.num_ops = defaultdict(int)

        # For collective communication, one barrier will be created for
        # each unique group of participants.
        self.barriers = set()
        self._rank = None

    def send(self, tensor: "torch.Tensor", peer_rank: int):
        # p2p operations are done via a shared memory channel, initialized in
        # `create_channel` of `TorchTensorType`
        pass

    def recv(
        self,
        shape: Tuple[int],
        dtype: "torch.dtype",
        peer_rank: int,
        allocator: Optional[TorchTensorAllocator] = None,
    ):
        # See the comment on `send`
        pass

    def allreduce(
        self,
        send_buf: "torch.Tensor",
        recv_buf: "torch.Tensor",
        op: ReduceOp = ReduceOp.SUM,
    ):
        all_ranks = [
            self.get_rank(actor_handle) for actor_handle in self.get_actor_handles()
        ]
        barrier_key = "barrier-collective-" + "-".join(map(str, sorted(all_ranks)))
        barrier = CPUCommBarrier.options(name=barrier_key, get_if_exists=True).remote(
            self._world_size
        )
        self.barriers.add(barrier)

        result = ray.get(
            barrier.wait_collective.remote(self.num_ops[barrier_key], send_buf, op)
        )
        assert recv_buf is not None, "Receiving buffer required for CPUCommunicator"
        recv_buf[:] = result[:]
        self.num_ops[barrier_key] += 1

    def destroy(self) -> None:
        for barrier in self.barriers:
            ray.kill(barrier)

    def initialize(self, rank: int) -> None:
        self._rank = rank

    def get_actor_handles(self) -> List["ray.actor.ActorHandle"]:
        return self._actor_handles

    def get_rank(self, actor: ray.actor.ActorHandle) -> int:
        """
        Return the given actor's rank in the CPU communicator.

        Args:
            actor: The actor handle to look up.
        """
        actor_ids = [a._ray_actor_id for a in self._actor_handles]
        try:
            rank = actor_ids.index(actor._ray_actor_id)
        except ValueError:
            raise ValueError("Actor is not in the CPUCommunicator group.")
        return rank

    def get_self_rank(self) -> Optional[int]:
        return self._rank

    def get_world_size(self) -> int:
        """
        Return the number of ranks in the CPU communicator.
        """
        return self._world_size

    def get_transport_name(self) -> str:
        return "cpu"

    def recv_stream(self):
        raise NotImplementedError

    def send_stream(self):
        raise NotImplementedError
