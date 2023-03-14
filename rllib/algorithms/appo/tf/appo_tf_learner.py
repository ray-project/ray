from dataclasses import dataclass
import numpy as np
from typing import Any, List, Mapping
import tree

from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.algorithms.appo.tf.appo_tf_rl_module import (OLD_ACTION_DIST_KEY, OLD_ACTION_DIST_LOGITS_KEY)
from ray.rllib.algorithms.impala.tf.vtrace_tf_v2 import make_time_major, vtrace_tf2
from ray.rllib.algorithms.impala.tf.impala_tf_learner import ImpalaHPs, ImpalaTfLearner
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED,
)
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import ResultDict, TensorType

_, tf, _ = try_import_tf()


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
        target_kl: The target kl divergence loss coefficient to use for the KL loss.
        kl_coeff: The coefficient to weight the KL divergence between the old policy
            and the target policy towards the total loss for module updates.

    """

    target_kl: float = 0.01
    kl_coeff: float = 0.1
    clip_param = 0.2


class AppoTfLearner(ImpalaTfLearner):
    """Implements APPO loss / update logic on top of ImpalaTfLearner.

    This class implements the APPO loss under `_compute_loss_per_module()` and 
        implements the target network and KL coefficient updates under 
        `additional_updates_per_module()`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_kl = self._hps.target_kl
        self.clip_param = self._hps.clip_param
        self.kl_coeff = self._hps.kl_coeff

    @override(TfLearner)
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        values = fwd_out[SampleBatch.VF_PREDS]
        target_policy_dist = fwd_out[SampleBatch.ACTION_DIST]
        old_target_policy_dist = fwd_out[OLD_ACTION_DIST_KEY]

        old_target_policy_actions_logp = old_target_policy_dist.logp(batch[SampleBatch.ACTIONS])
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

        # batch_size = tf.cast(target_actions_logp_time_major.shape[-1], tf.float32)

        # The policy gradients loss.
        is_ratio = tf.math.exp(behaviour_actions_logp_time_major - old_actions_logp_time_major)

        logp_ratio = is_ratio * tf.math.exp(target_actions_logp - behaviour_actions_logp_time_major)

        surrogate_loss = tf.math.minimum(
            pg_advantages * logp_ratio,
            (pg_advantages *
                tf.clip_by_value(logp_ratio, 1 - self.clip_param, 1 + self.clip_param)
            )
        )

        action_kl = old_target_policy_dist.kl(target_policy_dist)
        mean_kl_loss = tf.math.reduce_mean(action_kl)
        mean_pi_loss = - tf.math.reduce_mean(surrogate_loss)

        # The baseline loss.
        delta = values_time_major - vtrace_adjusted_target_values
        mean_vf_loss = 0.5 * tf.math.reduce_mean(delta**2)

        # The entropy loss.
        mean_entropy_loss = -tf.math.reduce_mean(target_actions_logp_time_major)

        # The summed weighted loss.
        total_loss = (
            mean_pi_loss + (mean_vf_loss * self.vf_loss_coeff) + (mean_entropy_loss * 
            self.entropy_coeff) + (mean_kl_loss * self.kl_coeff)
        )

        return {
            self.TOTAL_LOSS_KEY: total_loss,
            "mean_pi_loss": mean_pi_loss,
            "mean_vf_loss": mean_vf_loss,
            "mean_entropy_loss": mean_entropy_loss,
            "mean_kl_loss": mean_kl_loss,
        }
