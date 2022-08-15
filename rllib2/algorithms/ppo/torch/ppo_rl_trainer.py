#######################################################
########### PPO
#######################################################
from typing import Dict, Optional, Type, Union

import torch

from rllib2.core.torch.rl_module import TorchRLModule
from rllib2.core.torch.rl_trainer import TorchRLTrainer
from rllib2.core.torch.torch_rl_module import (
    PPOModuleOutput,
    PPORLModuleConfig,
    PPOTorchRLModule,
)


class PPOUnitTrainerConfig(UnitTrainerConfig):
    vf_clip_param: Optional[float] = None
    vf_loss_coeff: Optional[float] = None


class PPOTorchTrainer(TorchRLTrainer):
    def __init__(self, config: PPOUnitTrainerConfig):
        super().__init__(config)

    def make_optimizer(self) -> Dict[LossID, Optimizer]:
        config = self.config.optimizer_config
        return {"total_loss": torch.optim.Adam(self.model.parameters(), lr=config.lr)}

    def loss(
        self, train_batch: SampleBatch, fwd_train_dict: PPOModuleOutput
    ) -> Dict[LossID, torch.Tensor]:

        pi_out_cur = fwd_train_dict.pi_out_cur
        pi_out_prev = fwd_train_dict.pi_out_prev
        vf = fwd_train_dict.vf

        kl_coeff = self.model.kl_coeff

        # in-place operation, computes advantages and value targets
        compute_advantages(vf, train_batch, self.gamma, self.lambda_, ...)

        if kl_coeff:
            mean_kl_loss = KLDiv()(pi_out_prev, pi_out_cur)
        else:
            mean_kl_loss = torch.tensor(0.0, device=vf.device)

        mean_entropy = Entropy()(pi_out_cur)
        surrogate_loss = PPOClipLoss(clip_param=self.config["clip_params"])(train_batch)

        value_fn_out = 0
        mean_vf_loss = vf_loss_clipped = 0.0
        if vf:
            value_fn_out = vf.values.reduce("min")
            vf_loss = (value_fn_out - train_batch["vf_targets"]) ** 2
            vf_loss_clipped = torch.clamp(vf_loss, 0, self.config["vf_clip_param"])
            mean_vf_loss = reduce_mean_valid(vf_loss_clipped)

        total_loss = reduce_mean_valid(
            -surrogate_loss
            + self.config.vf_loss_coeff * vf_loss_clipped
            - self.config.entropy_coeff * mean_entropy
        )

        if kl_coeff:
            total_loss = kl_coeff * mean_kl_loss

        self.logger.tower_stats.update(
            total_loss=total_loss,
            mean_policy_loss=-surrogate_loss,
            mean_vf_loss=mean_vf_loss,
            vf_explained_var=explained_variance(
                train_batch[Postprocessing.VALUE_TARGETS], value_fn_out
            ),
            mean_entropy=mean_entropy,
            mean_kl_loss=mean_kl_loss,
        )

        return {"total_loss": total_loss}

    def update(self, train_batch, update_kl: bool = False):
        super().update(train_batch)

        if update_kl:
            self.update_kl()

    def update_kl(self):
        # Update the current KL value based on the recently measured value.
        sampled_kl = self.logger.tower_stats["mean_kl_loss"]
        kl_target = self.config["kl_target"]
        if sampled_kl > 2.0 * kl_target:
            self.model.kl_coeff *= 1.5
        elif sampled_kl < 0.5 * kl_target:
            self.model.kl_coeff *= 0.5
