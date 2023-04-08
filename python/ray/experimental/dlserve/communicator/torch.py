
import torch

from ray.experimental.dlserve.communicator.communicator import Communicator

class _FullfiledFuture:
    """A future that is already fullfilled."""

    def is_completed(self):
        return True

    def wait(self):
        pass

FULLFILLED_FUTURE = _FullfiledFuture()


class TorchBasedCommunicator(Communicator):
    def __init__(self, world_size: int, rank: int):
        assert torch.distributed.is_available()
        super().__init__(world_size, rank)
        # TODO: do torch distributed initialization here.

    def send(self, tensor: torch.Tensor, dest_rank: int, async_op: bool = False):
        if async_op:
            return torch.distributed.isend(tensor, dest_rank)
        else:
            torch.distributed.send(tensor, dest_rank)
            return FULLFILLED_FUTURE

    def recv(self, tensor, src_rank, async_op: bool = False):
        if async_op:
            return torch.distributed.irecv(tensor, src_rank)
        else:
            torch.distributed.recv(tensor, src_rank)
            return FULLFILLED_FUTURE

    def reconfigure(word_size: int, rank: int):
        pass