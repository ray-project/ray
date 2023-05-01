from typing import Dict, Mapping

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.algorithms.appo.appo_learner import (
    AppoLearner,
    LEARNER_RESULTS_CURR_KL_COEFF_KEY,
    LEARNER_RESULTS_KL_KEY,
    OLD_ACTION_DIST_KEY,
)
from ray.rllib.algorithms.impala.tf.vtrace_tf_v2 import make_time_major, vtrace_tf2
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY, ENTROPY_KEY
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.core.rl_module.marl_module import ModuleID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorType

_, tf, _ = try_import_tf()


class APPOTfLearner(TfLearner, AppoLearner):
    """Implements APPO loss / update logic on top of ImpalaTfLearner."""

    def __init__(self, *args, **kwargs):
        TfLearner.__init__(self, *args, **kwargs)
        AppoLearner.__init__(self, *args, **kwargs)

    @override(TfLearner)
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        values = fwd_out[SampleBatch.VF_PREDS]
        target_policy_dist = fwd_out[SampleBatch.ACTION_DIST]
        old_target_policy_dist = fwd_out[OLD_ACTION_DIST_KEY]
        old_target_policy_actions_logp = old_target_policy_dist.logp(
            batch[SampleBatch.ACTIONS]
        )
        behaviour_actions_logp = batch[SampleBatch.ACTION_LOGP]
        target_actions_logp = target_policy_dist.logp(batch[SampleBatch.ACTIONS])

        behaviour_actions_logp_time_major = make_time_major(
            behaviour_actions_logp,
            trajectory_len=self.hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self.hps.recurrent_seq_len,
            drop_last=self.hps.vtrace_drop_last_ts,
        )
        target_actions_logp_time_major = make_time_major(
            target_actions_logp,
            trajectory_len=self.hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self.hps.recurrent_seq_len,
            drop_last=self.hps.vtrace_drop_last_ts,
        )
        old_actions_logp_time_major = make_time_major(
            old_target_policy_actions_logp,
            trajectory_len=self.hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self.hps.recurrent_seq_len,
            drop_last=self.hps.vtrace_drop_last_ts,
        )
        values_time_major = make_time_major(
            values,
            trajectory_len=self.hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self.hps.recurrent_seq_len,
            drop_last=self.hps.vtrace_drop_last_ts,
        )
        bootstrap_value = values_time_major[-1]
        rewards_time_major = make_time_major(
            batch[SampleBatch.REWARDS],
            trajectory_len=self.hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self.hps.recurrent_seq_len,
            drop_last=self.hps.vtrace_drop_last_ts,
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
                    drop_last=self.hps.vtrace_drop_last_ts,
                ),
                dtype=tf.float32,
            )
        ) * self.hps.discount_factor
        vtrace_adjusted_target_values, pg_advantages = vtrace_tf2(
            target_action_log_probs=old_actions_logp_time_major,
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_value=bootstrap_value,
            clip_pg_rho_threshold=self.hps.vtrace_clip_pg_rho_threshold,
            clip_rho_threshold=self.hps.vtrace_clip_rho_threshold,
            discounts=discounts_time_major,
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

        action_kl = old_target_policy_dist.kl(target_policy_dist)
        mean_kl_loss = tf.math.reduce_mean(action_kl)
        mean_pi_loss = -tf.math.reduce_mean(surrogate_loss)

        # The baseline loss.
        delta = values_time_major - vtrace_adjusted_target_values
        mean_vf_loss = 0.5 * tf.math.reduce_mean(delta**2)

        # The entropy loss.
        mean_entropy_loss = -tf.math.reduce_mean(target_actions_logp_time_major)

        # The summed weighted loss.
        total_loss = (
            mean_pi_loss
            + (mean_vf_loss * self.hps.vf_loss_coeff)
            + (mean_entropy_loss * self.hps.entropy_coeff)
            + (mean_kl_loss * self.kl_coeffs[module_id])
        )

        return {
            self.TOTAL_LOSS_KEY: total_loss,
            POLICY_LOSS_KEY: mean_pi_loss,
            VF_LOSS_KEY: mean_vf_loss,
            ENTROPY_KEY: mean_entropy_loss,
            LEARNER_RESULTS_KL_KEY: mean_kl_loss,
            LEARNER_RESULTS_CURR_KL_COEFF_KEY: self.kl_coeffs[module_id],
        }

    @override(AppoLearner)
    def _update_module_target_networks(self, module_id: ModuleID):
        """Update the target policy of each module with the current policy.

        Do that update via polyak averaging.

        Args:
            module_id: The module whose target networks need to be updated.

        """
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

    def _update_module_kl_coeff(
        self, module_id: ModuleID, sampled_kls: Dict[ModuleID, float]
    ):
        """Dynamically update the KL loss coefficients of each module with.

        The update is completed using the mean KL divergence between the action
        distributions current policy and old policy of each module. That action
        distribution is computed during the most recent update/call to `compute_loss`.

        Args:
            module_id: The module whose KL loss coefficient to update.
            sampled_kls: The KL divergence between the action distributions of
                the current policy and old policy of each module.

        """
        if module_id in sampled_kls:
            sampled_kl = sampled_kls[module_id]
            # Update the current KL value based on the recently measured value.
            # Increase.
            if sampled_kl > 2.0 * self.hps.kl_target:
                self.kl_coeffs[module_id].assign(self.kl_coeffs[module_id] * 1.5)
            # Decrease.
            elif sampled_kl < 0.5 * self.hps.kl_target:
                self.kl_coeffs[module_id].assign(self.kl_coeffs[module_id] * 0.5)
