import abc
from typing import Any, Dict

import torch
from torch.optim import Optimizer

LossID = str
BatchType = "BatchType"


class TorchTrainer:
    def __init__(self):
        self._optimizers: Dict[LossID, Optimizer] = self.make_optimizer()

    @abc.abstractmethod
    def make_optimizer(self) -> Dict[LossID, Optimizer]:
        raise NotImplementedError

    def compute_grads_and_apply_if_needed(
        self,
        batch: BatchType,
        fwd_out,
        loss_out: Dict[LossID, torch.Tensor],
        apply_grad: bool = True,
    ) -> Any:

        for loss_key, loss_value in loss_out.items():
            self._optimizers[loss_key].zero_grad()
            loss_value.backward()

            if apply_grad:
                self._optimizers[loss_key].step()

