from typing import Any, Dict, Optional

from ray.rllib.algorithms.marwil.marwil import MARWILConfig
from ray.rllib.algorithms.marwil.marwil_learner import (
    LEARNER_RESULTS_MOVING_AVG_SQD_ADV_NORM_KEY,
    LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY,
    MARWILLearner,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import explained_variance
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class MARWILTorchLearner(MARWILLearner, TorchLearner):
    """Implements torch-specific MARWIL loss on top of MARWILLearner.

    This class implements the MARWIL loss under `self.compute_loss_for_module()`.
    """

    def compute_loss_for_module(
        self,
        *,
        module_id: str,
        config: Optional[MARWILConfig] = None,
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType]
    ) -> TensorType:
        module = self.module[module_id].unwrapped()

        # Possibly apply masking to some sub loss terms and to the total loss term
        # at the end. Masking could be used for RNN-based model (zero padded `batch`)
        # and for PPO's batched value function (and bootstrap value) computations,
        # for which we add an additional (artificial) timestep to each episode to
        # simplify the actual computation.
        if Columns.LOSS_MASK in batch:
            num_valid = torch.sum(batch[Columns.LOSS_MASK])

            def possibly_masked_mean(data_):
                return torch.sum(data_[batch[Columns.LOSS_MASK]]) / num_valid

        else:
            possibly_masked_mean = torch.mean

        action_dist_class_train = module.get_train_action_dist_cls()
        curr_action_dist = action_dist_class_train.from_logits(
            fwd_out[Columns.ACTION_DIST_INPUTS]
        )

        log_probs = curr_action_dist.logp(batch[Columns.ACTIONS])

        # If beta is zero, we fall back to BC.
        if config.beta == 0.0:
            # Value function's loss term.
            mean_vf_loss = 0.0
            # Policy's loss term.
            exp_weighted_advantages = 1.0
        # Otherwise, compute advantages.
        else:
            # cumulative_rewards = batch[Columns.ADVANTAGES]
            value_fn_out = module.compute_values(
                batch, embeddings=fwd_out.get(Columns.EMBEDDINGS)
            )
            advantages = batch[Columns.VALUE_TARGETS] - value_fn_out
            advantages_squared_mean = possibly_masked_mean(torch.pow(advantages, 2.0))

            # Compute the value loss.
            mean_vf_loss = 0.5 * advantages_squared_mean

            # Compute the policy loss.
            self.moving_avg_sqd_adv_norms_per_module[module_id] = (
                config.moving_average_sqd_adv_norm_update_rate
                * (
                    advantages_squared_mean.detach()
                    - self.moving_avg_sqd_adv_norms_per_module[module_id]
                )
                + self.moving_avg_sqd_adv_norms_per_module[module_id]
            )
            # Exponentially weighted advantages.
            # TODO (simon): Check, if we need the mask here.
            exp_weighted_advantages = torch.exp(
                config.beta
                * (
                    advantages
                    / (
                        1e-8
                        + torch.pow(
                            self.moving_avg_sqd_adv_norms_per_module[module_id], 0.5
                        )
                    )
                )
            ).detach()

        # Note, using solely a log-probability loss term tends to push the action
        # distributions to have very low entropy, which results in worse performance
        # specifically in unknown situations.
        # Scaling the loss term with the logarithm of the action distribution's
        # standard deviation encourages stochasticity in the policy.
        if config.bc_logstd_coeff > 0.0:
            log_stds = possibly_masked_mean(curr_action_dist.log_std, dim=1)
        else:
            log_stds = 0.0

        # Compute the policy loss.
        policy_loss = -possibly_masked_mean(
            exp_weighted_advantages * (log_probs + config.bc_logstd_coeff * log_stds)
        )

        # Compute the total loss.
        total_loss = policy_loss + config.vf_coeff * mean_vf_loss

        # Log import loss stats. In case of the BC loss this is simply
        # the policy loss.
        if config.beta == 0.0:
            self.metrics.log_value((module_id, POLICY_LOSS_KEY), policy_loss, window=1)
        # Log more stats, if using the MARWIL loss.
        else:
            ma_sqd_adv_norms = self.moving_avg_sqd_adv_norms_per_module[module_id]
            self.metrics.log_dict(
                {
                    POLICY_LOSS_KEY: policy_loss,
                    VF_LOSS_KEY: mean_vf_loss,
                    LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY: explained_variance(
                        batch[Postprocessing.VALUE_TARGETS], value_fn_out
                    ),
                    LEARNER_RESULTS_MOVING_AVG_SQD_ADV_NORM_KEY: ma_sqd_adv_norms,
                },
                key=module_id,
                window=1,  # <- single items (should not be mean/ema-reduced over time).
            )

        # Return the total loss.
        return total_loss
