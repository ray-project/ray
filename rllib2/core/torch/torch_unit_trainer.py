import abc
from dataclasses import dataclass
from typing import Dict, Type, Union

import torch
from torch.optim import Optimizer

from rllib2.data.sample_batch import SampleBatch
from rllib2.models.torch.torch_rl_module import RLModuleConfig, TorchRLModule

"""
Some examples of pre-defined RLlib unit_trainers --> See under algorithms
"""


@dataclass
class UnitTrainerConfig:
    model_class: Optional[Union[str, Type[TorchRLModule]]] = None
    model_config: Optional[RLModuleConfig] = None
    optimizer_config: Optional[Dict[str, Any]] = None


class TorchUnitTrainer:
    def __init__(self, config):
        self._config = config

        # register the RLModule model
        self._model = self._make_model()

        # register optimizers
        self._optimizers: Dict[str, Optimizer] = self.make_optimizer()

    @property
    def config(self) -> UnitTrainerConfig:
        return self._config

    @property
    def model(self) -> TorchRLModule:
        return self._model

    @property
    def optimizers(self) -> Dict[str, Optimizer]:
        return self._optimizers

    @property
    def default_rl_module(self) -> Union[str, Type[TorchRLModule]]:
        return ""

    @abc.abstractmethod
    def make_optimizer(self) -> Dict[str, Optimizer]:
        raise NotImplementedError

    @abc.abstractmethod
    def _make_model(self) -> TorchRLModule:
        config = self.config
        rl_module_class = config.get("model_class", self.default_rl_module())
        rl_module_config = config["model_config"]

        # import rl_module_class with rl_module_config
        # TODO
        rl_module: TorchRLModule = None
        return rl_module

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
