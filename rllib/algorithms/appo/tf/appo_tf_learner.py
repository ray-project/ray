from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Mapping

from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.algorithms.appo.tf.appo_tf_rl_module import OLD_ACTION_DIST_KEY
from ray.rllib.algorithms.impala.tf.vtrace_tf_v2 import make_time_major, vtrace_tf2
from ray.rllib.algorithms.impala.tf.impala_tf_learner import ImpalaHPs, ImpalaTfLearner
from ray.rllib.core.rl_module.marl_module import ModuleID
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics import LEARNER_STATS_KEY, ALL_MODULES
from ray.rllib.utils.typing import TensorType

_, tf, _ = try_import_tf()


LEARNER_RESULTS_KL_KEY = "mean_kl_loss"


@dataclass
class AppoHPs(ImpalaHPs):
    """Hyper-parameters for IMPALA.

    Attributes:
        rollout_frag_or_episode_len: The length of a rollout fragment or episode.
            Used when making SampleBatches time major for computing loss.
        recurrent_seq_len: The length of a recurrent sequence. Used when making
            SampleBatches time major for computing loss.
        discount_factor: The discount factor to use for computing returns.
        vtrace_clip_rho_threshold: The rho threshold to use for clipping the
            importance weights.
        vtrace_clip_pg_rho_threshold: The rho threshold to use for clipping the
            importance weights when computing the policy_gradient loss.
        vtrace_drop_last_ts: Whether to drop the last timestep when computing the loss.
            This is useful for stabilizing the loss.
            NOTE: This shouldn't be True when training on environments where the rewards
            come at the end of the episode.
        vf_loss_coeff: The amount to weight the value function loss by when computing
            the total loss.
        entropy_coeff: The amount to weight the average entropy of the actions in the
            SampleBatch towards the total_loss for module updates. The higher this
            coefficient, the more that the policy network will be encouraged to output
            distributions with higher entropy/std deviation, which will encourage
            greater exploration.
        kl_target: The target kl divergence loss coefficient to use for the KL loss.
        kl_coeff: The coefficient to weight the KL divergence between the old policy
            and the target policy towards the total loss for module updates.
        tau: The factor by which to update the target policy network towards
                the current policy network. Can range between 0 and 1.

    """

    kl_target: float = 0.01
    kl_coeff: float = 0.1
    clip_param = 0.2
    tau = 1.0


class APPOTfLearner(ImpalaTfLearner):
    """Implements APPO loss / update logic on top of ImpalaTfLearner.

    This class implements the APPO loss under `_compute_loss_per_module()` and
        implements the target network and KL coefficient updates under
        `additional_updates_per_module()`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kl_target = self._hps.kl_target
        self.clip_param = self._hps.clip_param
        self.kl_coeffs = defaultdict(lambda: self._hps.kl_coeff)
        self.kl_coeff = self._hps.kl_coeff
        self.tau = self._hps.tau
        self.sampled_kls = {}

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
            trajectory_len=self.rollout_frag_or_episode_len,
            recurrent_seq_len=self.recurrent_seq_len,
            drop_last=self.vtrace_drop_last_ts,
        )
        target_actions_logp_time_major = make_time_major(
            target_actions_logp,
            trajectory_len=self.rollout_frag_or_episode_len,
            recurrent_seq_len=self.recurrent_seq_len,
            drop_last=self.vtrace_drop_last_ts,
        )
        old_actions_logp_time_major = make_time_major(
            old_target_policy_actions_logp,
            trajectory_len=self.rollout_frag_or_episode_len,
            recurrent_seq_len=self.recurrent_seq_len,
            drop_last=self.vtrace_drop_last_ts,
        )

        values_time_major = make_time_major(
            values,
            trajectory_len=self.rollout_frag_or_episode_len,
            recurrent_seq_len=self.recurrent_seq_len,
            drop_last=self.vtrace_drop_last_ts,
        )
        bootstrap_value = values_time_major[-1]
        rewards_time_major = make_time_major(
            batch[SampleBatch.REWARDS],
            trajectory_len=self.rollout_frag_or_episode_len,
            recurrent_seq_len=self.recurrent_seq_len,
            drop_last=self.vtrace_drop_last_ts,
        )

        # the discount factor that is used should be gamma except for timesteps where
        # the episode is terminated. In that case, the discount factor should be 0.
        discounts_time_major = (
            1.0
            - tf.cast(
                make_time_major(
                    batch[SampleBatch.TERMINATEDS],
                    trajectory_len=self.rollout_frag_or_episode_len,
                    recurrent_seq_len=self.recurrent_seq_len,
                    drop_last=self.vtrace_drop_last_ts,
                ),
                dtype=tf.float32,
            )
        ) * self.discount_factor
        vtrace_adjusted_target_values, pg_advantages = vtrace_tf2(
            target_action_log_probs=old_actions_logp_time_major,
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_value=bootstrap_value,
            clip_pg_rho_threshold=self.vtrace_clip_pg_rho_threshold,
            clip_rho_threshold=self.vtrace_clip_rho_threshold,
            discounts=discounts_time_major,
        )

        # The policy gradients loss.
        is_ratio = tf.math.exp(
            behaviour_actions_logp_time_major - old_actions_logp_time_major
        )
        logp_ratio = is_ratio * tf.math.exp(
            target_actions_logp_time_major - behaviour_actions_logp_time_major
        )

        surrogate_loss = tf.math.minimum(
            pg_advantages * logp_ratio,
            (
                pg_advantages
                * tf.clip_by_value(logp_ratio, 1 - self.clip_param, 1 + self.clip_param)
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
            + (mean_vf_loss * self.vf_loss_coeff)
            + (mean_entropy_loss * self.entropy_coeff)
            + (mean_kl_loss * self.kl_coeffs[module_id])
        )

        return {
            self.TOTAL_LOSS_KEY: total_loss,
            "policy_loss": mean_pi_loss,
            "vf_loss": mean_vf_loss,
            "entropy": mean_entropy_loss,
            LEARNER_RESULTS_KL_KEY: mean_kl_loss,
        }

    def compile_results(
        self,
        batch: MultiAgentBatch,
        fwd_out: Mapping[str, Any],
        postprocessed_loss: Mapping[str, Any],
        postprocessed_gradients: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        results = super().compile_results(
            batch, fwd_out, postprocessed_loss, postprocessed_gradients
        )
        module_ids = set(results.keys()) - {ALL_MODULES}
        self.sampled_kls = {
            module_id: results[module_id][LEARNER_STATS_KEY][LEARNER_RESULTS_KL_KEY]
            for module_id in module_ids
        }
        return results

    @override(ImpalaTfLearner)
    def remove_module(self, module_id: str):
        super().remove_module(module_id)
        self.kl_coeffs.pop(module_id)

    def _update_module_target_pi(self):
        """Update the target policy of each module with the current policy.

        Do that update via polyak averaging.

        """
        module_ids = self.module.keys()
        for module_id in module_ids:
            module = self.module[module_id]
            old_encoder = module.old_encoder
            old_pi = module.old_pi
            current_encoder = module.encoder
            current_pi = module.pi

            encoder_old_current_pair = {"old": old_encoder, "current": current_encoder}
            pi_old_current_pair = {"old": old_pi, "current": current_pi}
            for network_pair in [encoder_old_current_pair, pi_old_current_pair]:
                for old_var, current_var in zip(
                    network_pair["old"].variables, network_pair["current"].variables
                ):
                    updated_var = self.tau * current_var + (1.0 - self.tau) * old_var
                    old_var.assign(updated_var)

    def _update_module_kl_coeff(self):
        """Dynamically update the KL loss coefficients of each module with.

        The update is completed using the mean KL divergence between the action
        distributions current policy and old policy of each module. That action
        distribution is computed during the most recent update/call to `compute_loss`.

        """
        for module_id, sampled_kl in self.sampled_kls.items():
            # Update the current KL value based on the recently measured value.
            # Increase.
            if sampled_kl > 2.0 * self.kl_target:
                self.kl_coeffs[module_id] *= 1.5
            # Decrease.
            elif sampled_kl < 0.5 * self.kl_target:
                self.kl_coeffs[module_id] *= 0.5

    def additional_update(self, **kwargs) -> Mapping[ModuleID, Any]:
        self._update_module_target_pi()
        self._update_module_kl_coeff()
        return {}
