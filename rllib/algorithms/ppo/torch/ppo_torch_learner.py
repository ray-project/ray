import logging
from typing import Any, Dict, Mapping

from ray.rllib.algorithms.ppo.ppo_learner import (
    LEARNER_RESULTS_KL_KEY,
    LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY,
    LEARNER_RESULTS_CURR_KL_COEFF_KEY,
    LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY,
    LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY,
    PPOLearner,
)
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY, ENTROPY_KEY
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import explained_variance
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class PPOTorchLearner(PPOLearner, TorchLearner):
    """Implements torch-specific PPO loss logic on top of PPOLearner.

    This class implements the ppo loss under `_compute_loss_per_module()`.
    """

    @override(TorchLearner)
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        # TODO (Kourosh): batch type is NestedDict.
        # TODO (Kourosh): We may or may not user module_id. For example if we have an
        # agent based learning rate scheduler, we may want to use module_id to get the
        # learning rate for that agent.
        # TODO (Kourosh): come back to RNNs later

        curr_action_dist = fwd_out[SampleBatch.ACTION_DIST]
        action_dist_class = type(fwd_out[SampleBatch.ACTION_DIST])
        prev_action_dist = action_dist_class.from_logits(
            batch[SampleBatch.ACTION_DIST_INPUTS]
        )

        logp_ratio = torch.exp(
            fwd_out[SampleBatch.ACTION_LOGP] - batch[SampleBatch.ACTION_LOGP]
        )

        # Only calculate kl loss if necessary (kl-coeff > 0.0).
        if self.hps.kl_coeff > 0.0:
            action_kl = prev_action_dist.kl(curr_action_dist)
            mean_kl_loss = torch.mean(action_kl)
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

        curr_entropy = fwd_out["entropy"]
        mean_entropy = torch.mean(curr_entropy)

        surrogate_loss = torch.min(
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
            -surrogate_loss
            + self.hps.vf_loss_coeff * vf_loss_clipped
            - self.hps.entropy_coeff * curr_entropy
        )

        # Add mean_kl_loss (already processed through `reduce_mean_valid`),
        # if necessary.
        if self.hps.kl_coeff > 0.0:
            total_loss += self.curr_kl_coeffs_per_module[module_id] * mean_kl_loss

        return {
            self.TOTAL_LOSS_KEY: total_loss,
            POLICY_LOSS_KEY: -torch.mean(surrogate_loss),
            VF_LOSS_KEY: mean_vf_loss,
            LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY: mean_vf_unclipped_loss,
            LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY: explained_variance(
                batch[Postprocessing.VALUE_TARGETS], value_fn_out
            ),
            ENTROPY_KEY: mean_entropy,
            LEARNER_RESULTS_KL_KEY: mean_kl_loss,
            LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY: self.hps.entropy_coeff,
            LEARNER_RESULTS_CURR_KL_COEFF_KEY: (
                self.curr_kl_coeffs_per_module[module_id]
            ),
        }

    @override(PPOLearner)
    def additional_update_per_module(
        self, module_id: ModuleID, sampled_kl_values: dict, timestep: int
    ) -> Dict[str, Any]:
        assert sampled_kl_values, "Sampled KL values are empty."

        results = super().additional_update_per_module(
            module_id,
            sampled_kl_values,
            timestep=timestep,
        )

        sampled_kl = sampled_kl_values[module_id]
        curr_val = self.curr_kl_coeffs_per_module[module_id]
        if sampled_kl > 2.0 * self.hps.kl_target:
            # TODO (Kourosh) why not 2?
            curr_val.data *= 1.5
        elif sampled_kl < 0.5 * self.hps.kl_target:
            curr_val.data *= 0.5

        results.update({"kl_coeff": curr_val})
        return results

    @override(PPOLearner)
    def _get_kl_variable(self, value: float) -> Any:
        return torch.tensor(
            value,
            requires_grad=False,
            device=self._device,
            dtype=torch.float32,
        )
