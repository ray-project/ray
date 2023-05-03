import logging
from typing import Any, Mapping

from ray.rllib.algorithms.ppo.ppo_learner import PPOLearner
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
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
            total_loss += self.curr_kl_coeff * mean_kl_loss

        return {
            self.TOTAL_LOSS_KEY: total_loss,
            "policy_loss": -torch.mean(surrogate_loss),
            "vf_loss": mean_vf_loss,
            "unclipped_vf_loss": mean_vf_unclipped_loss,
            "vf_explained_var": explained_variance(
                batch[Postprocessing.VALUE_TARGETS], value_fn_out
            ),
            "entropy": mean_entropy,
            "kl": mean_kl_loss,
            "entropy_coeff": self.hps.entropy_coeff,
            "cur_kl_coeff": self.curr_kl_coeff,
        }

    @override(PPOLearner)
    def _get_kl_variable(self, value: float) -> Any:
        return torch.tensor(
            value,
            requires_grad=False,
            device=self._device,
            dtype=torch.float32,
        )

    @override(PPOLearner)
    def _set_kl_coeff(self, value: float):
        self.curr_kl_coeff.data = torch.tensor(
            value,
            dtype=torch.float32,
            device=self.curr_kl_coeff.device,
        )
