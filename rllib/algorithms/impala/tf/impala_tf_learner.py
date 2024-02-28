from typing import Dict

import tree
from ray.rllib.algorithms.impala.impala import ImpalaConfig
from ray.rllib.algorithms.impala.impala_learner import ImpalaLearner
from ray.rllib.algorithms.impala.tf.vtrace_tf_v2 import make_time_major, vtrace_tf2
from ray.rllib.core.learner.learner import ENTROPY_KEY
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.core.models.base import CRITIC, ENCODER_OUT
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import ModuleID, TensorType

_, tf, _ = try_import_tf()


class ImpalaTfLearner(ImpalaLearner, TfLearner):
    """Implements the IMPALA loss function in tensorflow."""

    @override(TfLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: ImpalaConfig,
        batch: NestedDict,
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        action_dist_class_train = self.module[module_id].get_train_action_dist_cls()
        target_policy_dist = action_dist_class_train.from_logits(
            fwd_out[SampleBatch.ACTION_DIST_INPUTS]
        )
        values = fwd_out[SampleBatch.VF_PREDS]

        behaviour_actions_logp = batch[SampleBatch.ACTION_LOGP]
        target_actions_logp = target_policy_dist.logp(batch[SampleBatch.ACTIONS])
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
        rewards_time_major = make_time_major(
            batch[SampleBatch.REWARDS],
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        values_time_major = make_time_major(
            values,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        if self.config.uses_new_env_runners:
            bootstrap_values = batch[SampleBatch.VALUES_BOOTSTRAPPED]
        else:
            bootstrap_values_time_major = make_time_major(
                batch[SampleBatch.VALUES_BOOTSTRAPPED],
                trajectory_len=rollout_frag_or_episode_len,
                recurrent_seq_len=recurrent_seq_len,
            )
            bootstrap_values = bootstrap_values_time_major[-1]

        # the discount factor that is used should be gamma except for timesteps where
        # the episode is terminated. In that case, the discount factor should be 0.
        discounts_time_major = (
            1.0
            - tf.cast(
                make_time_major(
                    batch[SampleBatch.TERMINATEDS],
                    trajectory_len=rollout_frag_or_episode_len,
                    recurrent_seq_len=recurrent_seq_len,
                ),
                dtype=tf.float32,
            )
        ) * config.gamma

        # Note that vtrace will compute the main loop on the CPU for better performance.
        vtrace_adjusted_target_values, pg_advantages = vtrace_tf2(
            target_action_log_probs=target_actions_logp_time_major,
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            discounts=discounts_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_values=bootstrap_values,
            clip_pg_rho_threshold=config.vtrace_clip_pg_rho_threshold,
            clip_rho_threshold=config.vtrace_clip_rho_threshold,
        )

        # Sample size is T x B, where T is the trajectory length and B is the batch size
        batch_size = tf.cast(target_actions_logp_time_major.shape[-1], tf.float32)

        # The policy gradients loss.
        pi_loss = -tf.reduce_sum(target_actions_logp_time_major * pg_advantages)
        mean_pi_loss = pi_loss / batch_size

        # The baseline loss.
        delta = values_time_major - vtrace_adjusted_target_values
        vf_loss = 0.5 * tf.reduce_sum(delta**2)
        mean_vf_loss = vf_loss / batch_size

        # The entropy loss.
        mean_entropy_loss = -tf.reduce_mean(target_policy_dist.entropy())

        # The summed weighted loss.
        total_loss = (
            pi_loss
            + vf_loss * config.vf_loss_coeff
            + (
                mean_entropy_loss
                * self.entropy_coeff_schedulers_per_module[
                    module_id
                ].get_current_value()
            )
        )

        # Register important loss stats.
        self.register_metrics(
            module_id,
            {
                "pi_loss": mean_pi_loss,
                "vf_loss": mean_vf_loss,
                ENTROPY_KEY: -mean_entropy_loss,
            },
        )
        # Return the total loss.
        return total_loss

    @override(ImpalaLearner)
    def _compute_values(self, batch):
        infos = batch.pop(SampleBatch.INFOS, None)
        batch = tree.map_structure(lambda s: tf.convert_to_tensor(s), batch)
        if infos is not None:
            batch[SampleBatch.INFOS] = infos

        # TODO (sven): Make multi-agent capable.
        module = self.module[DEFAULT_POLICY_ID].unwrapped()

        # Shared encoder.
        encoder_outs = module.encoder(batch)
        # Value head.
        vf_out = module.vf(encoder_outs[ENCODER_OUT][CRITIC])
        # Squeeze out last dimension (single node value head).
        return tf.squeeze(vf_out, -1)
