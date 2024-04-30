import logging
from typing import Dict

from ray.rllib.algorithms.ppo.ppo import (
    LEARNER_RESULTS_KL_KEY,
    LEARNER_RESULTS_CURR_KL_COEFF_KEY,
    LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY,
    LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY,
    PPOConfig,
)
from ray.rllib.algorithms.ppo.ppo_learner import PPOLearner
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY, ENTROPY_KEY
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import explained_variance
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import ModuleID, TensorType

_, tf, _ = try_import_tf()
logger = logging.getLogger(__name__)


class PPOTfLearner(PPOLearner, TfLearner):
    """Implements tf-specific PPO loss logic on top of PPOLearner.

    This class implements the ppo loss under `self.compute_loss_for_module()`.
    """

    @override(TfLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: PPOConfig,
        batch: NestedDict,
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        # TODO (Kourosh): batch type is NestedDict.
        # TODO (Kourosh): We may or may not user module_id. For example if we have an
        # agent based learning rate scheduler, we may want to use module_id to get the
        # learning rate for that agent.

        # RNN case: Mask away 0-padded chunks at end of time axis.
        if self.module[module_id].is_stateful():
            # In the RNN case, we expect incoming tensors to be padded to the maximum
            # sequence length. We infer the max sequence length from the actions
            # tensor.
            maxlen = tf.math.reduce_max(batch[Columns.SEQ_LENS])
            mask = tf.sequence_mask(batch[Columns.SEQ_LENS], maxlen)

            def possibly_masked_mean(t):
                return tf.reduce_mean(tf.boolean_mask(t, mask))

        # non-RNN case: No masking.
        else:
            mask = None
            possibly_masked_mean = tf.reduce_mean

        action_dist_class_train = self.module[module_id].get_train_action_dist_cls()
        action_dist_class_exploration = self.module[
            module_id
        ].get_exploration_action_dist_cls()
        curr_action_dist = action_dist_class_train.from_logits(
            fwd_out[Columns.ACTION_DIST_INPUTS]
        )
        prev_action_dist = action_dist_class_exploration.from_logits(
            batch[Columns.ACTION_DIST_INPUTS]
        )

        logp_ratio = tf.exp(
            curr_action_dist.logp(batch[Columns.ACTIONS]) - batch[Columns.ACTION_LOGP]
        )

        # Only calculate kl loss if necessary (kl-coeff > 0.0).
        if config.use_kl_loss:
            action_kl = prev_action_dist.kl(curr_action_dist)
            mean_kl_loss = possibly_masked_mean(action_kl)
        else:
            mean_kl_loss = tf.constant(0.0, dtype=logp_ratio.dtype)

        curr_entropy = curr_action_dist.entropy()
        mean_entropy = possibly_masked_mean(curr_entropy)

        surrogate_loss = tf.minimum(
            batch[Postprocessing.ADVANTAGES] * logp_ratio,
            batch[Postprocessing.ADVANTAGES]
            * tf.clip_by_value(
                logp_ratio, 1 - config.clip_param, 1 + config.clip_param
            ),
        )

        # Compute a value function loss.
        if config.use_critic:
            value_fn_out = fwd_out[Columns.VF_PREDS]
            vf_loss = tf.math.square(value_fn_out - batch[Postprocessing.VALUE_TARGETS])
            vf_loss_clipped = tf.clip_by_value(vf_loss, 0, config.vf_clip_param)
            mean_vf_loss = possibly_masked_mean(vf_loss_clipped)
            mean_vf_unclipped_loss = possibly_masked_mean(vf_loss)
        # Ignore the value function.
        else:
            value_fn_out = tf.constant(0.0, dtype=surrogate_loss.dtype)
            mean_vf_unclipped_loss = tf.constant(0.0, dtype=surrogate_loss.dtype)
            vf_loss_clipped = mean_vf_loss = tf.constant(
                0.0, dtype=surrogate_loss.dtype
            )

        total_loss = possibly_masked_mean(
            -surrogate_loss
            + config.vf_loss_coeff * vf_loss_clipped
            - (
                self.entropy_coeff_schedulers_per_module[module_id].get_current_value()
                * curr_entropy
            )
        )

        # Add mean_kl_loss (already processed through `reduce_mean_valid`),
        # if necessary.
        if config.use_kl_loss:
            total_loss += self.curr_kl_coeffs_per_module[module_id] * mean_kl_loss

        # Log important loss stats.
        self.metrics.log_dict(
            {
                POLICY_LOSS_KEY: -tf.reduce_mean(surrogate_loss),
                VF_LOSS_KEY: mean_vf_loss,
                LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY: mean_vf_unclipped_loss,
                LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY: explained_variance(
                    batch[Postprocessing.VALUE_TARGETS], value_fn_out
                ),
                ENTROPY_KEY: mean_entropy,
                LEARNER_RESULTS_KL_KEY: mean_kl_loss,
                # "advantages": possibly_masked_mean(batch[Columns.ADVANTAGES]),
                # "values": possibly_masked_mean(batch[Columns.VF_PREDS]),
                # "value_targets": possibly_masked_mean(
                #    batch[Columns.VALUE_TARGETS]
                # ),
            },
            key=module_id,
            window=1,  # <- single items (should not be mean/ema-reduced over time).
        )
        # Return the total loss.
        return total_loss

    @override(PPOLearner)
    def additional_update_for_module(
        self,
        *,
        module_id: ModuleID,
        config: PPOConfig,
        timestep: int,
        sampled_kl_values: dict,
    ) -> None:

        super().additional_update_for_module(
            module_id=module_id,
            config=config,
            timestep=timestep,
        )

        # Update KL coefficient.
        if config.use_kl_loss:
            assert sampled_kl_values, "Sampled KL values are empty."
            sampled_kl = sampled_kl_values[module_id]
            curr_var = self.curr_kl_coeffs_per_module[module_id]
            if sampled_kl > 2.0 * config.kl_target:
                # TODO (Kourosh) why not 2?
                curr_var.assign(curr_var * 1.5)
            elif sampled_kl < 0.5 * config.kl_target:
                curr_var.assign(curr_var * 0.5)

            # Log the updated KL-coeff value.
            self.metrics.log_value(
                (module_id, LEARNER_RESULTS_CURR_KL_COEFF_KEY),
                curr_var.numpy(),
                window=1,
            )
