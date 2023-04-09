
import torch

from ray.experimental.parallel_ml.communicator.communicator import (Communicator, FULLFILLED_FUTURE)


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