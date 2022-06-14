#######################################################
########### PPO
#######################################################


from rllib2.core.torch.torch_unit_trainer import TorchUnitTrainer, UnitTrainerConfig
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


