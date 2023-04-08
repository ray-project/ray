import asyncio
from collections import defaultdict, deque
from typing import List

import ray
import torch
from ray.experimental.dlserve.communicator.communicator import (
    FULLFILLED_FUTURE,
    Communicator,
)
from ray.types import ObjectRef


@ray.remote
class CommunicationRegistry(object):
    def __init__(self):
        self._sending_queue = defaultdict(deque)

    async def send(
        self, obj_refs: List[ObjectRef], from_rank: int, to_rank: int
    ) -> None:
        self._get_queue(from_rank, to_rank).extend(obj_refs)

    async def recv(self, from_rank: int, to_rank: int) -> List[ObjectRef]:
        queue = self._get_queue(from_rank, to_rank)
        while len(queue) == 0:
            await asyncio.sleep(0.01)
        return [queue.popleft()]

    def _get_queue(self, from_rank: int, to_rank: int) -> deque:
        return self._sending_queue.get((from_rank, to_rank))


class NaiveCommunicator(Communicator):
    """A naive communicator that uses ray.put and ray.get to send and receive tensors."""

    def __init__(self, world_size: int, rank: int, group_name: str = "default_group"):
        self._communication_registry = CommunicationRegistry.options(
            name=group_name, get_if_exists=True
        ).remote()
        super().__init__(world_size, rank)

    def send(self, tensor: torch.Tensor, dest_rank: int, async_op: bool = False):
        obj_ref = ray.put(tensor)
        send_ref = self._communication_registry.send.remote(
            [obj_ref], self._rank, dest_rank
        )
        if async_op:
            pass
        else:
            ray.get(send_ref)
            return FULLFILLED_FUTURE

    def recv(self, tensor: torch.Tensor, src_rank: int, async_op: bool = False):
        receive_ref = self._communication_registry.recv.remote(src_rank, self._rank)
        # TODO: can we really do async_op?
        received_tensor = ray.get(receive_ref)
        tensor.copy_(received_tensor)
        return FULLFILLED_FUTURE

    def reconfigure(word_size: int, rank: int):
        pass
