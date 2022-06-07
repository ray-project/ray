from typing import Dict
import abc

import torch
from torch.optim import Optimizer

from rllib2.data.sample_batch import SampleBatch

from .torch_rl_module import TorchRLModule


class UnitTrainer:

    def __init__(self, config):
        self._config = config

        # register the RLModule model
        self._model = self.make_model()

        # register optimizers
        self._optimizers: Dict[str, Optimizer] = self.make_optimizer()

    @property
    def config(self):
        return self._config

    @property
    def model(self):
        return self._model

    @property
    def optimizers(self):
        return self._optimizers

    @abc.abstractmethod
    def make_optimizer(self) -> Dict[str, Optimizer]:
        raise NotImplementedError

    @abc.abstractmethod
    def make_model(self) -> TorchRLModule:
        raise NotImplementedError

    @abc.abstractmethod
    def loss(self, train_batch: SampleBatch, fwd_train_dict) -> Dict[str, torch.Tensor]:
        """
        Computes the loss for each sub-module of the algorithm and returns the loss
        tensor computed for each loss that needs to get back-propagated and updated
        according to the corresponding optimizer.

        This method should use self.model.forward_train() to compute the forward-pass
        tensors required for training.

        Args:
            train_batch: SampleBatch to train with.

        Returns:
            Dict of optimizer names map their loss tensors.
        """
        raise NotImplementedError

    def update(self, train_batch: SampleBatch):

        self.model.train()
        fwd_train_dict = self.model.forward_train(train_batch)
        loss_dict = self.loss(train_batch, fwd_train_dict)

        for loss_key, loss_value in loss_dict.items():
            self._optimizers[loss_key].zero_grad()
            loss_value.backward()
            self._optimizers[loss_key].step()