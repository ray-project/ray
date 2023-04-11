import logging
from typing import Mapping, Any

from ray.rllib.algorithms.ppo.ppo_base_learner import PPOBaseLearner
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import explained_variance
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TensorType
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.models.torch.torch_distributions import TorchCategorical
from ray.rllib.examples.rlhf.ppo_ft.utils import masked_mean

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class RLHFPPOTorchLearner(PPOTorchLearner):

    @override(PPOTorchLearner)
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        """Extention of PPO loss function to support RLHF.

        This customization adds attention mask to loss calculation.
        It also adds the ptx-loss term introduced in InstructGPT paper for making sure 
        the model is aligned with the pre-trained model.
        """

        # make sure all the coefficients are on the same device as the model
        if self.kl_coeff.device != self._device:
            self.kl_coeff = self.kl_coeff.to(self._device)

        curr_action_dist = fwd_out[SampleBatch.ACTION_DIST]
        prev_action_dist = TorchCategorical(logits=batch[SampleBatch.ACTIONS]["logits"])
        attention_mask = batch[SampleBatch.ACTIONS]["attention_mask"]

        cur_logp = curr_action_dist.logp(batch[SampleBatch.ACTIONS]["sequence"])
        prev_logp = prev_action_dist.logp(batch[SampleBatch.ACTIONS]["sequence"])

        logp_ratio_unmasked = torch.exp(cur_logp - prev_logp)
        logp_ratio = masked_mean(logp_ratio_unmasked, attention_mask, dim=-1)

        # Only calculate kl loss if necessary (kl-coeff > 0.0).
        if self.hps.kl_coeff > 0.0:
            action_kl = prev_action_dist.kl(curr_action_dist)
            mean_kl_loss = masked_mean(action_kl, attention_mask, dim=-1).mean()
            if mean_kl_loss.isinf():
                logger.warning(
                    "KL divergence is non-finite, this will likely destabilize "
                    "your model and the training process. Action(s) in a "
                    "specific state have near-zero probability. "
                    "This can happen naturally in deterministic "
                    "environments where the optimal policy has zero mass "
                    "for a specific action. To fix this issue, consider "
                    "setting the coefficient for the KL loss term to "
                    "zero or increasing policy entropy."
                )
        else:
            mean_kl_loss = torch.tensor(0.0, device=logp_ratio.device)

        curr_entropy_unmasked = curr_action_dist.entropy()
        curr_entropy = masked_mean(curr_entropy_unmasked, attention_mask, dim=-1)
        mean_entropy = curr_entropy.mean()

        surrogate_loss = - torch.min(
            batch[Postprocessing.ADVANTAGES] * logp_ratio,
            batch[Postprocessing.ADVANTAGES]
            * torch.clamp(logp_ratio, 1 - self.hps.clip_param, 1 + self.hps.clip_param),
        )

        # Compute a value function loss.
        if self.hps.use_critic:
            value_fn_out = fwd_out[SampleBatch.VF_PREDS]
            vf_loss = torch.pow(value_fn_out - batch[Postprocessing.VALUE_TARGETS], 2.0)
            vf_loss_clipped = torch.clamp(vf_loss, 0, self.hps.vf_clip_param)
            mean_vf_loss = torch.mean(vf_loss_clipped)
            mean_vf_unclipped_loss = torch.mean(vf_loss)
        # Ignore the value function.
        else:
            value_fn_out = torch.tensor(0.0).to(surrogate_loss.device)
            mean_vf_unclipped_loss = torch.tensor(0.0).to(surrogate_loss.device)
            vf_loss_clipped = mean_vf_loss = torch.tensor(0.0).to(surrogate_loss.device)

        total_loss = torch.mean(
            surrogate_loss
            + self.hps.vf_loss_coeff * vf_loss_clipped
            - self.entropy_coeff * curr_entropy
        )

        # Add mean_kl_loss (already processed through `reduce_mean_valid`),
        # if necessary.
        if self.hps.kl_coeff > 0.0:
            total_loss += self.kl_coeff * mean_kl_loss

        return {
            self.TOTAL_LOSS_KEY: total_loss,
            "policy_loss": torch.mean(surrogate_loss),
            "vf_loss": mean_vf_loss,
            "unclipped_vf_loss": mean_vf_unclipped_loss,
            "vf_explained_var": explained_variance(
                batch[Postprocessing.VALUE_TARGETS], value_fn_out
            ),
            "entropy": mean_entropy,
            "kl": mean_kl_loss,
            "entropy_coeff": self.entropy_coeff,
            "cur_kl_coeff": self.kl_coeff,
            "mean_reward_total": batch[SampleBatch.REWARDS].mean(),
            "mean_reward_rm": batch[SampleBatch.INFOS]["r_align"].mean(),
            "mean_reward_kl": batch[SampleBatch.INFOS]["r_kl"].mean(),
        }
