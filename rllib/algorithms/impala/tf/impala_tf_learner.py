from dataclasses import dataclass
import numpy as np
from typing import Any, List, Mapping
import tree

from ray.rllib import SampleBatch
from ray.rllib.algorithms.impala.tf.vtrace_tf_v2 import make_time_major, vtrace_tf2
from ray.rllib.core.learner.learner import LearnerHPs
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import ResultDict, TensorType

_, tf, _ = try_import_tf()


@dataclass
class ImpalaHPs(LearnerHPs):
    rollout_frag_or_episode_len: int = None
    recurrent_seq_len: int = None
    discount_factor: float = 0.99
    vtrace_clip_rho_threshold: float = 1.0
    vtrace_clip_pg_rho_threshold: float = 1.0
    vtrace_drop_last_ts: bool = True
    vf_loss_coeff: float = 0.5
    entropy_coeff: float = 0.01


class ImpalaTfLearner(TfLearner):
    """Implements IMPALA loss / update logic on top of TfLearner.

    This class implements the IMPALA loss under `_compute_loss_per_module()`.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.vtrace_clip_rho_threshold = self._hps.vtrace_clip_rho_threshold
        self.vtrace_clip_pg_rho_threshold = self._hps.vtrace_clip_pg_rho_threshold
        self.vtrace_drop_last_ts = self._hps.vtrace_drop_last_ts
        self.vf_loss_coeff = self._hps.vf_loss_coeff
        self.entropy_coeff = self._hps.entropy_coeff
        self.rollout_frag_or_episode_len = self._hps.rollout_frag_or_episode_len
        self.recurrent_seq_len = self._hps.recurrent_seq_len
        self.discount_factor = self._hps.discount_factor
        assert (
            self.rollout_frag_or_episode_len is not None
            or self.recurrent_seq_len is not None
        ) and not (self.rollout_frag_or_episode_len and self.recurrent_seq_len), (
            "Either rollout_frag_or_episode_len or recurrent_seq_len"
            " must be set in the IMPALA HParams. "
        )

    @override(TfLearner)
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        values = fwd_out[SampleBatch.VF_PREDS]
        target_policy_dist = fwd_out[SampleBatch.ACTION_DIST]

        # this is probably a horribly inefficient way to do this. I should be able to
        # compute this in a batch fashion
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

        # how to compute discouts?
        # should they be pre computed?
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
            target_action_log_probs=target_actions_logp_time_major,
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_value=bootstrap_value,
            clip_pg_rho_threshold=self.vtrace_clip_pg_rho_threshold,
            clip_rho_threshold=self.vtrace_clip_rho_threshold,
            discounts=discounts_time_major,
        )

        # The policy gradients loss.
        pi_loss = -tf.reduce_sum(target_actions_logp_time_major * pg_advantages)
        mean_pi_loss = -tf.reduce_mean(target_actions_logp_time_major * pg_advantages)

        # The baseline loss.
        delta = values_time_major - vtrace_adjusted_target_values
        vf_loss = 0.5 * tf.reduce_sum(tf.math.pow(delta, 2.0))
        mean_vf_loss = 0.5 * tf.reduce_mean(tf.math.pow(delta, 2.0))

        # The entropy loss.
        entropy_loss = -tf.reduce_sum(target_actions_logp_time_major)

        # The summed weighted loss.
        total_loss = (
            pi_loss + vf_loss * self.vf_loss_coeff + entropy_loss * self.entropy_coeff
        )
        return {
            self.TOTAL_LOSS_KEY: total_loss,
            "pi_loss": mean_pi_loss,
            "vf_loss": mean_vf_loss,
        }

    @override(TfLearner)
    def compile_results(
        self,
        batch: SampleBatch,
        fwd_out: Mapping[str, Any],
        postprocessed_loss: Mapping[str, Any],
        postprocessed_gradients: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        results = super().compile_results(
            batch, fwd_out, postprocessed_loss, postprocessed_gradients
        )
        results["agent_steps_trained"] = batch.agent_steps()
        results["env_steps_trained"] = batch.env_steps()
        return results


def _reduce_impala_results(results: List[ResultDict]) -> ResultDict:
    """Takes the average of all the leaves in the result dict

    Args:
        results: list of result dicts to average

    Returns:
        Averaged result dict
    """
    result = tree.map_structure(lambda *x: np.mean(x), *results)
    agent_steps_trained = sum([r["agent_steps_trained"] for r in results])
    env_steps_trained = sum([r["env_steps_trained"] for r in results])
    result["agent_steps_trained"] = agent_steps_trained
    result["env_steps_trained"] = env_steps_trained
    return result
