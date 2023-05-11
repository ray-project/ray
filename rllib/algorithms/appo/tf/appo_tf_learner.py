from typing import Any, Dict, Mapping

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.algorithms.appo.appo_learner import (
    AppoLearner,
    LEARNER_RESULTS_CURR_KL_COEFF_KEY,
    LEARNER_RESULTS_KL_KEY,
    OLD_ACTION_DIST_LOGITS_KEY,
)
from ray.rllib.algorithms.impala.tf.vtrace_tf_v2 import make_time_major, vtrace_tf2
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY, ENTROPY_KEY
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.core.rl_module.marl_module import ModuleID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorType

_, tf, _ = try_import_tf()


class APPOTfLearner(AppoLearner, TfLearner):
    """Implements APPO loss / update logic on top of ImpalaTfLearner."""

    @override(TfLearner)
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        values = fwd_out[SampleBatch.VF_PREDS]
        action_dist_cls_train = self._module[module_id].get_train_action_dist_cls()
        target_policy_dist = action_dist_cls_train.from_logits(
            fwd_out[SampleBatch.ACTION_DIST_INPUTS]
        )
        old_target_policy_dist = action_dist_cls_train.from_logits(
            fwd_out[OLD_ACTION_DIST_LOGITS_KEY]
        )
        old_target_policy_actions_logp = old_target_policy_dist.logp(
            batch[SampleBatch.ACTIONS]
        )
        behaviour_actions_logp = batch[SampleBatch.ACTION_LOGP]
        target_actions_logp = target_policy_dist.logp(batch[SampleBatch.ACTIONS])

        behaviour_actions_logp_time_major = make_time_major(
            behaviour_actions_logp,
            trajectory_len=self.hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self.hps.recurrent_seq_len,
        )
        target_actions_logp_time_major = make_time_major(
            target_actions_logp,
            trajectory_len=self.hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self.hps.recurrent_seq_len,
        )
        old_actions_logp_time_major = make_time_major(
            old_target_policy_actions_logp,
            trajectory_len=self.hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self.hps.recurrent_seq_len,
        )
        values_time_major = make_time_major(
            values,
            trajectory_len=self.hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self.hps.recurrent_seq_len,
        )
        bootstrap_value = values_time_major[-1]
        rewards_time_major = make_time_major(
            batch[SampleBatch.REWARDS],
            trajectory_len=self.hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self.hps.recurrent_seq_len,
        )

        # the discount factor that is used should be gamma except for timesteps where
        # the episode is terminated. In that case, the discount factor should be 0.
        discounts_time_major = (
            1.0
            - tf.cast(
                make_time_major(
                    batch[SampleBatch.TERMINATEDS],
                    trajectory_len=self.hps.rollout_frag_or_episode_len,
                    recurrent_seq_len=self.hps.recurrent_seq_len,
                ),
                dtype=tf.float32,
            )
        ) * self.hps.discount_factor

        # Note that vtrace will compute the main loop on the CPU for better performance.
        vtrace_adjusted_target_values, pg_advantages = vtrace_tf2(
            target_action_log_probs=old_actions_logp_time_major,
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            discounts=discounts_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_value=bootstrap_value,
            clip_pg_rho_threshold=self.hps.vtrace_clip_pg_rho_threshold,
            clip_rho_threshold=self.hps.vtrace_clip_rho_threshold,
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
                    logp_ratio, 1 - self.hps.clip_param, 1 + self.hps.clip_param
                )
            ),
        )

        if self.hps.use_kl_loss:
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
            + (mean_vf_loss * self.hps.vf_loss_coeff)
            + (
                mean_entropy_loss
                * self.entropy_coeff_scheduler.get_current_value(module_id)
            )
            + (mean_kl_loss * self.curr_kl_coeffs_per_module[module_id])
        )

        return {
            self.TOTAL_LOSS_KEY: total_loss,
            POLICY_LOSS_KEY: mean_pi_loss,
            VF_LOSS_KEY: mean_vf_loss,
            ENTROPY_KEY: -mean_entropy_loss,
            LEARNER_RESULTS_KL_KEY: mean_kl_loss,
            LEARNER_RESULTS_CURR_KL_COEFF_KEY: (
                self.curr_kl_coeffs_per_module[module_id]
            ),
        }

    @override(AppoLearner)
    def _update_module_target_networks(self, module_id: ModuleID):
        module = self.module[module_id]

        target_current_network_pairs = module.get_target_network_pairs()
        for target_network, current_network in target_current_network_pairs:
            for old_var, current_var in zip(
                target_network.variables, current_network.variables
            ):
                updated_var = (
                    self.hps.tau * current_var + (1.0 - self.hps.tau) * old_var
                )
                old_var.assign(updated_var)

    @override(AppoLearner)
    def _update_module_kl_coeff(
        self, module_id: ModuleID, sampled_kl: float
    ) -> Dict[str, Any]:
        # Update the current KL value based on the recently measured value.
        # Increase.
        kl_coeff_var = self.curr_kl_coeffs_per_module[module_id]

        if sampled_kl > 2.0 * self.hps.kl_target:
            # TODO (Kourosh) why not *2.0?
            kl_coeff_var.assign(kl_coeff_var * 1.5)
        # Decrease.
        elif sampled_kl < 0.5 * self.hps.kl_target:
            kl_coeff_var.assign(kl_coeff_var * 0.5)

        return {LEARNER_RESULTS_CURR_KL_COEFF_KEY: kl_coeff_var.numpy()}
