import logging
from typing import Any, Dict, Mapping

from ray.rllib.algorithms.ppo.ppo_learner import (
    LEARNER_RESULTS_KL_KEY,
    LEARNER_RESULTS_CURR_KL_COEFF_KEY,
    LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY,
    LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY,
    PPOLearner,
)
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY, ENTROPY_KEY
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import explained_variance
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TensorType


_, tf, _ = try_import_tf()
logger = logging.getLogger(__name__)


class PPOTfLearner(PPOLearner, TfLearner):
    """Implements tf-specific PPO loss logic on top of PPOLearner.

    This class implements the ppo loss under `_compute_loss_per_module()`.
    """

    @override(TfLearner)
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        # TODO (Kourosh): batch type is NestedDict.
        # TODO (Kourosh): We may or may not user module_id. For example if we have an
        # agent based learning rate scheduler, we may want to use module_id to get the
        # learning rate for that agent.
        # TODO (Kourosh): come back to RNNs later

        action_dist_class_train = self.module[module_id].get_train_action_dist_cls()
        action_dist_class_exploration = self.module[
            module_id
        ].get_exploration_action_dist_cls()
        curr_action_dist = action_dist_class_train.from_logits(
            fwd_out[SampleBatch.ACTION_DIST_INPUTS]
        )
        prev_action_dist = action_dist_class_exploration.from_logits(
            batch[SampleBatch.ACTION_DIST_INPUTS]
        )

        logp_ratio = tf.exp(
            curr_action_dist.logp(batch[SampleBatch.ACTIONS])
            - batch[SampleBatch.ACTION_LOGP]
        )

        # Only calculate kl loss if necessary (kl-coeff > 0.0).
        if self.hps.kl_coeff > 0.0:
            action_kl = prev_action_dist.kl(curr_action_dist)
            mean_kl_loss = tf.reduce_mean(action_kl)
            if tf.math.is_inf(mean_kl_loss):
                logger.warning(
                    "KL divergence is non-finite, this will likely destabilize "
                    "your model and the training process. Action(s) in a "
                    "specific state have near-zero probability. "
                    "This can happen naturally in deterministic "
                    "environments where the optimal policy has zero mass "
                    "for a specific action. To fix this issue, consider "
                    "setting `kl_coeff` to 0.0 or increasing `entropy_coeff` in your "
                    "config."
                )
        else:
            mean_kl_loss = tf.constant(0.0, dtype=logp_ratio.dtype)

        curr_entropy = curr_action_dist.entropy()
        mean_entropy = tf.reduce_mean(curr_entropy)

        surrogate_loss = tf.minimum(
            batch[Postprocessing.ADVANTAGES] * logp_ratio,
            batch[Postprocessing.ADVANTAGES]
            * tf.clip_by_value(
                logp_ratio, 1 - self.hps.clip_param, 1 + self.hps.clip_param
            ),
        )

        # Compute a value function loss.
        if self.hps.use_critic:
            value_fn_out = fwd_out[SampleBatch.VF_PREDS]
            vf_loss = tf.math.square(value_fn_out - batch[Postprocessing.VALUE_TARGETS])
            vf_loss_clipped = tf.clip_by_value(vf_loss, 0, self.hps.vf_clip_param)
            mean_vf_loss = tf.reduce_mean(vf_loss_clipped)
            mean_vf_unclipped_loss = tf.reduce_mean(vf_loss)
        # Ignore the value function.
        else:
            value_fn_out = tf.constant(0.0, dtype=surrogate_loss.dtype)
            mean_vf_unclipped_loss = tf.constant(0.0, dtype=surrogate_loss.dtype)
            vf_loss_clipped = mean_vf_loss = tf.constant(
                0.0, dtype=surrogate_loss.dtype
            )

        total_loss = tf.reduce_mean(
            -surrogate_loss
            + self.hps.vf_loss_coeff * vf_loss_clipped
            - self.entropy_coeff_scheduler.get_current_value(module_id) * curr_entropy
        )

        # Add mean_kl_loss (already processed through `reduce_mean_valid`),
        # if necessary.
        if self.hps.kl_coeff > 0.0:
            total_loss += self.curr_kl_coeffs_per_module[module_id] * mean_kl_loss

        return {
            self.TOTAL_LOSS_KEY: total_loss,
            POLICY_LOSS_KEY: -tf.reduce_mean(surrogate_loss),
            VF_LOSS_KEY: mean_vf_loss,
            LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY: mean_vf_unclipped_loss,
            LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY: explained_variance(
                batch[Postprocessing.VALUE_TARGETS], value_fn_out
            ),
            ENTROPY_KEY: mean_entropy,
            LEARNER_RESULTS_KL_KEY: mean_kl_loss,
        }

    @override(PPOLearner)
    def additional_update_per_module(
        self, module_id: ModuleID, sampled_kl_values: dict, timestep: int
    ) -> Dict[str, Any]:
        assert sampled_kl_values, "Sampled KL values are empty."

        results = super().additional_update_per_module(
            module_id,
            sampled_kl_values=sampled_kl_values,
            timestep=timestep,
        )

        # Update KL coefficient.
        sampled_kl = sampled_kl_values[module_id]
        curr_var = self.curr_kl_coeffs_per_module[module_id]
        if sampled_kl > 2.0 * self.hps.kl_target:
            # TODO (Kourosh) why not 2?
            curr_var.assign(curr_var * 1.5)
        elif sampled_kl < 0.5 * self.hps.kl_target:
            curr_var.assign(curr_var * 0.5)
        results.update({LEARNER_RESULTS_CURR_KL_COEFF_KEY: curr_var.numpy()})

        return results
