from abc import ABC

import torch


class Communicator(ABC):
    """Provides MPI style communication primitives."""

    def __init__(self, world_size: int, rank: int):
        self.world_size = world_size
        self.rank = rank

    def send(self, tensor: torch.Tensor, dest_rank: int, async_op: bool = False):
        pass

    def recv(self, tensor: torch.Tensor, src_rank: int, async_op: bool = False):
        pass

    def reconfigure(word_size: int, rank: int):
        pass


class _FullfiledFuture:
    """A future that is already fullfilled."""

    def is_completed(self):
        return True

    def wait(self):
        pass


FULLFILLED_FUTURE = _FullfiledFuture()
