from abc import ABC
import torch

class Communicator(ABC):
    """Provides MPI style communication primitives.
    """
    def __init__(self, world_size: int, rank: int):
        self.world_size = world_size
        self.rank = rank

    def send(self, tensor: torch.Tensor, dest_rank: int, async_op: bool = False):
        pass

    def recv(self, tensor: torch.Tensor, src_rank: int, async_op: bool = False):
        pass

    def reconfigure(word_size: int, rank: int):
        pass


class TorchBaseCommunicator(Communicator):
    def __init__(self, world_size: int, rank: int):
        assert torch.distributed.is_available()
        super().__init__(world_size, rank)
        # TODO: do torch distributed initialization here.


    def send(self, tensor: torch.Tensor, dest_rank: int, async_op: bool = False):
        if async_op:
            torch.distributed.isend(tensor, dest_rank)
        else:
            torch.distributed.send(tensor, dest_rank)


    def recv(self, tensor, src_rank, async_op: bool = False):
        if async_op:
            torch.distributed.irecv(tensor, src_rank)
        else:
            torch.distributed.recv(tensor, src_rank)

    def reconfigure(word_size: int, rank: int):
        pass