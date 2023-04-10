import os

import torch
from ray.experimental.parallel_ml.communicator.communicator import (
    FULLFILLED_FUTURE,
    Communicator,
)


class TorchBasedCommunicator(Communicator):
    def __init__(self, world_size: int, rank: int, master_addr: str):
        assert torch.distributed.is_available()
        super().__init__(world_size, rank)
        os.environ["MASTER_ADDR"] = master_addr
        os.environ["MASTER_PORT"] = "29500"
        self._initialzed = False

    def _lazy_init(self):
        if not self._initialzed:
            torch.distributed.init_process_group(
                backend="nccl", world_size=self.world_size, rank=self.rank
            )
        self._initialzed = True

    def send(self, tensor: torch.Tensor, dest_rank: int, async_op: bool = False):
        self._lazy_init()
        if async_op:
            return torch.distributed.isend(tensor, dest_rank)
        else:
            torch.distributed.send(tensor, dest_rank)
            return FULLFILLED_FUTURE

    def recv(self, tensor, src_rank, async_op: bool = False):
        self._lazy_init()
        if async_op:
            return torch.distributed.irecv(tensor, src_rank)
        else:
            torch.distributed.recv(tensor, src_rank)
            return FULLFILLED_FUTURE

    def reconfigure(word_size: int, rank: int):
        pass
