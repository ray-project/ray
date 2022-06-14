from typing import Type, Union
from dataclasses import dataclass
from typing import Dict
import abc

import torch
from torch.optim import Optimizer

from rllib2.data.sample_batch import SampleBatch
from rllib2.models.torch.torch_rl_module import TorchRLModule, RLModuleConfig

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
        return ''

    @abc.abstractmethod
    def make_optimizer(self) -> Dict[str, Optimizer]:
        raise NotImplementedError

    @abc.abstractmethod
    def _make_model(self) -> TorchRLModule:
        config = self.config
        rl_module_class = config.get('model_class', self.default_rl_module())
        rl_module_config = config['model_config']

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


"""
Some examples of pre-defined RLlib unit_trainers
"""

#######################################################
########### PPO
#######################################################
from rllib2.core.torch.torch_rl_module import (
    PPOTorchRLModule, PPORLModuleConfig, PPOModuleOutput
)

class PPOUnitTrainerConfig(UnitTrainerConfig):
    vf_clip_param: Optional[float] = None
    vf_loss_coeff: Optional[float] = None



class PPOUnitTrainer(TorchUnitTrainer):

    def __init__(self, config: PPOUnitTrainerConfig):
        super().__init__(config)

    def default_rl_module(self) -> Union[str, Type[TorchRLModule]]:
        return PPOTorchRLModule

    def make_optimizer(self) -> Dict[str, Optimizer]:
        config = self.config.optimizer_config
        return {'total_loss': torch.optim.Adam(self.model.parameters(), lr=config.lr)}

    def loss(self, train_batch: SampleBatch, fwd_train_dict: PPOModuleOutput) -> Dict[str, torch.Tensor]:

        pi_out_cur = fwd_train_dict.pi_out_cur
        pi_out_prev = fwd_train_dict.pi_out_prev
        vf = fwd_train_dict.vf

        kl_coeff = self.model.kl_coeff


        logp_ratio = torch.exp(
            pi_out_cur.log_prob(train_batch[SampleBatch.ACTIONS])
            - train_batch[SampleBatch.ACTION_LOGP]
        )

        if kl_coeff:
            action_kl = pi_out_prev.kl(pi_out_cur)
            mean_kl_loss = reduce_mean_valid(action_kl)
        else:
            mean_kl_loss = torch.tensor(0.0, device=logp_ratio.device)

        curr_entropy = pi_out_cur.entropy()
        mean_entropy = reduce_mean_valid(curr_entropy)

        surrogate_loss = torch.min(
            train_batch[Postprocessing.ADVANTAGES] * logp_ratio,
            train_batch[Postprocessing.ADVANTAGES]
            * torch.clamp(
                logp_ratio, 1 - self.config["clip_param"], 1 + self.config["clip_param"]
            ),
        )

        mean_policy_loss = reduce_mean_valid(-surrogate_loss)

        value_fn_out = 0
        mean_vf_loss = vf_loss_clipped = 0.0
        if vf:
            value_fn_out = vf.values.reduce('min')
            vf_loss = (value_fn_out -
                       train_batch[Postprocessing.VALUE_TARGETS])**2
            vf_loss_clipped = torch.clamp(vf_loss, 0, self.config["vf_clip_param"])
            mean_vf_loss = reduce_mean_valid(vf_loss_clipped)

        total_loss = reduce_mean_valid(
            -surrogate_loss
            + self.config.vf_loss_coeff * vf_loss_clipped
            - self.config.entropy_coeff * curr_entropy
        )

        if kl_coeff:
            total_loss = kl_coeff * mean_kl_loss


        self.logger.tower_stats.update(
            total_loss=total_loss,
            mean_policy_loss=mean_policy_loss,
            mean_vf_loss=mean_vf_loss,
            vf_explained_var=explained_variance(train_batch[Postprocessing.VALUE_TARGETS], value_fn_out),
            mean_entropy=mean_entropy,
            mean_kl_loss=mean_kl_loss,
        )

        return {'total_loss': total_loss}


