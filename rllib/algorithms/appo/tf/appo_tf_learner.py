from typing import Dict

from ray.rllib.algorithms.appo.appo import (
    APPOConfig,
    LEARNER_RESULTS_CURR_KL_COEFF_KEY,
    LEARNER_RESULTS_KL_KEY,
    OLD_ACTION_DIST_LOGITS_KEY,
)
from ray.rllib.algorithms.appo.appo_learner import APPOLearner
from ray.rllib.algorithms.impala.tf.impala_tf_learner import IMPALATfLearner
from ray.rllib.algorithms.impala.tf.vtrace_tf_v2 import make_time_major, vtrace_tf2
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY, ENTROPY_KEY
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import ModuleID, TensorType

_, tf, _ = try_import_tf()


class APPOTfLearner(APPOLearner, IMPALATfLearner):
    """Implements APPO loss / update logic on top of IMPALATfLearner."""

    @override(IMPALATfLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: APPOConfig,
        batch: Dict,
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        values = fwd_out[Columns.VF_PREDS]
        action_dist_cls_train = self._module[module_id].get_train_action_dist_cls()
        target_policy_dist = action_dist_cls_train.from_logits(
            fwd_out[Columns.ACTION_DIST_INPUTS]
        )
        old_target_policy_dist = action_dist_cls_train.from_logits(
            fwd_out[OLD_ACTION_DIST_LOGITS_KEY]
        )
        old_target_policy_actions_logp = old_target_policy_dist.logp(
            batch[Columns.ACTIONS]
        )
        behaviour_actions_logp = batch[Columns.ACTION_LOGP]
        target_actions_logp = target_policy_dist.logp(batch[Columns.ACTIONS])
        rollout_frag_or_episode_len = config.get_rollout_fragment_length()
        recurrent_seq_len = None

        behaviour_actions_logp_time_major = make_time_major(
            behaviour_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        target_actions_logp_time_major = make_time_major(
            target_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        old_actions_logp_time_major = make_time_major(
            old_target_policy_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        rewards_time_major = make_time_major(
            batch[Columns.REWARDS],
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        values_time_major = make_time_major(
            values,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        if config.enable_env_runner_and_connector_v2:
            bootstrap_values = batch[Columns.VALUES_BOOTSTRAPPED]
        else:
            bootstrap_values_time_major = make_time_major(
                batch[Columns.VALUES_BOOTSTRAPPED],
                trajectory_len=rollout_frag_or_episode_len,
                recurrent_seq_len=recurrent_seq_len,
            )
            bootstrap_values = bootstrap_values_time_major[-1]

        # The discount factor that is used should be gamma except for timesteps where
        # the episode is terminated. In that case, the discount factor should be 0.
        discounts_time_major = (
            1.0
            - tf.cast(
                make_time_major(
                    batch[Columns.TERMINATEDS],
                    trajectory_len=rollout_frag_or_episode_len,
                    recurrent_seq_len=recurrent_seq_len,
                ),
                dtype=tf.float32,
            )
        ) * config.gamma

        # Note that vtrace will compute the main loop on the CPU for better performance.
        vtrace_adjusted_target_values, pg_advantages = vtrace_tf2(
            target_action_log_probs=old_actions_logp_time_major,
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            discounts=discounts_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_values=bootstrap_values,
            clip_pg_rho_threshold=config.vtrace_clip_pg_rho_threshold,
            clip_rho_threshold=config.vtrace_clip_rho_threshold,
        )

        # The policy gradients loss.
        is_ratio = tf.clip_by_value(
            tf.math.exp(
                behaviour_actions_logp_time_major - old_actions_logp_time_major
            ),
            0.0,
            2.0,
        )
        logp_ratio = is_ratio * tf.math.exp(
            target_actions_logp_time_major - behaviour_actions_logp_time_major
        )

        surrogate_loss = tf.math.minimum(
            pg_advantages * logp_ratio,
            (
                pg_advantages
                * tf.clip_by_value(
                    logp_ratio,
                    1 - config.clip_param,
                    1 + config.clip_param,
                )
            ),
        )

        if config.use_kl_loss:
            action_kl = old_target_policy_dist.kl(target_policy_dist)
            mean_kl_loss = tf.math.reduce_mean(action_kl)
        else:
            mean_kl_loss = 0.0
        mean_pi_loss = -tf.math.reduce_mean(surrogate_loss)

        # The baseline loss.
        delta = values_time_major - vtrace_adjusted_target_values
        mean_vf_loss = 0.5 * tf.math.reduce_mean(delta**2)

        # The entropy loss.
        mean_entropy_loss = -tf.math.reduce_mean(target_policy_dist.entropy())

        # The summed weighted loss.
        total_loss = (
            mean_pi_loss
            + (mean_vf_loss * config.vf_loss_coeff)
            + (
                mean_entropy_loss
                * self.entropy_coeff_schedulers_per_module[
                    module_id
                ].get_current_value()
            )
            + (mean_kl_loss * self.curr_kl_coeffs_per_module[module_id])
        )

        # Register all important loss stats.
        # Note that our MetricsLogger (self.metrics) is currently in tensor-mode,
        # meaning that it allows us to even log in-graph/compiled tensors through
        # its `log_...()` APIs.
        self.metrics.log_dict(
            {
                POLICY_LOSS_KEY: mean_pi_loss,
                VF_LOSS_KEY: mean_vf_loss,
                ENTROPY_KEY: -mean_entropy_loss,
                LEARNER_RESULTS_KL_KEY: mean_kl_loss,
                LEARNER_RESULTS_CURR_KL_COEFF_KEY: (
                    self.curr_kl_coeffs_per_module[module_id]
                ),
            },
            key=module_id,
            window=1,  # <- single items (should not be mean/ema-reduced over time).
        )
        # Return the total loss.
        return total_loss

    @override(APPOLearner)
    def _update_module_kl_coeff(self, module_id: ModuleID, config: APPOConfig) -> None:
        # Update the current KL value based on the recently measured value.
        # Increase.
        kl = convert_to_numpy(self.metrics.peek((module_id, LEARNER_RESULTS_KL_KEY)))
        kl_coeff_var = self.curr_kl_coeffs_per_module[module_id]

        if kl > 2.0 * config.kl_target:
            # TODO (Kourosh) why not *2.0?
            kl_coeff_var.assign(kl_coeff_var * 1.5)
        # Decrease.
        elif kl < 0.5 * config.kl_target:
            kl_coeff_var.assign(kl_coeff_var * 0.5)

        self.metrics.log_value(
            (module_id, LEARNER_RESULTS_CURR_KL_COEFF_KEY),
            kl_coeff_var.numpy(),
            window=1,
        )
