"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from typing import Any, Dict, Mapping

import numpy as np

from ray.rllib.core.learner.learner import (
    POLICY_LOSS_KEY,
    VF_LOSS_KEY,
    ENTROPY_KEY,
)
from ray.rllib.core.rl_module.marl_module import ModuleID
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_tfp
from ray.rllib.utils.tf_utils import symlog, two_hot
from ray.rllib.utils.typing import TensorType

_, tf, _ = try_import_tf()
tfp = try_import_tfp()


class DreamerV3TfLearner(TfLearner):
    """Implements DreamerV3 losses and update logic in TensorFlow.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @override(TfLearner)
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        # World model losses.
        prediction_losses = self._compute_world_model_prediction_losses(
            rewards=batch["rewards"],
            continues=(1.0 - batch["is_terminated"]),
            fwd_out=fwd_out,
        )

        L_dyn_B_T, L_rep_B_T = (
            self._compute_world_model_dynamics_and_representation_loss(fwd_out=fwd_out)
        )

        L_pred_B_T = prediction_losses["prediction_loss_B_T"]
        L_pred = tf.reduce_mean(L_pred_B_T)

        L_decoder_B_T = prediction_losses["decoder_loss_B_T"]
        L_decoder = tf.reduce_mean(L_decoder_B_T)

        # Two-hot reward loss.
        L_reward_two_hot_B_T = prediction_losses["reward_loss_two_hot_B_T"]
        L_reward_two_hot = tf.reduce_mean(L_reward_two_hot_B_T)
        # TEST: Out of interest, compare with simplge -log(p) loss for individual
        # rewards using the FiniteDiscrete distribution. This should be very close
        # to the two-hot reward loss.
        # L_reward_logp_B_T = prediction_losses["reward_loss_logp_B_T"]
        # L_reward_logp = tf.reduce_mean(L_reward_logp_B_T)

        L_continue_B_T = prediction_losses["continue_loss_B_T"]
        L_continue = tf.reduce_mean(L_continue_B_T)

        L_dyn = tf.reduce_mean(L_dyn_B_T)
        L_rep = tf.reduce_mean(L_rep_B_T)
        # Make sure values for L_rep and L_dyn are the same (they only differ in their gradients).
        tf.assert_equal(L_dyn, L_rep)

        # Compute the actual total loss using fixed weights described in [1] eq. 4.
        L_world_model_total_B_T = 1.0 * L_pred_B_T + 0.5 * L_dyn_B_T + 0.1 * L_rep_B_T

        # Sum up timesteps, and average over batch (see eq. 4 in [1]).
        L_world_model_total = tf.reduce_mean(L_world_model_total_B_T)

        # Dream trajectories starting in all internal states (h+z) that were
        # computed during world model training.
        dream_data = self.module[module_id].dreamer_model.dream_trajectory(
            start_states={
                "h": world_model_forward_train_outs["h_states_BxT"],
                "z": world_model_forward_train_outs["z_states_BxT"],
            },
            start_is_terminated=is_terminated,
            timesteps_H=self._hps.horizon_H,
            gamma=self._hps.gamma,
        )

        value_targets = self._compute_value_targets(
            # Learn critic in symlog'd space.
            rewards_t0_to_H_BxT=dream_data["rewards_dreamed_t0_to_H_B"],
            intrinsic_rewards_t1_to_H_BxT=(
                dream_data["rewards_intrinsic_t1_to_H_B"] if self._hps.use_curiosity else None
            ),
            continues_t0_to_H_BxT=dream_data["continues_dreamed_t0_to_H_B"],
            value_predictions_t0_to_H_BxT=dream_data["values_dreamed_t0_to_H_B"],
        )
        critic_loss_results = self._compute_critic_loss(
            dream_data=dream_data,
            value_targets=value_targets,
        )
        if self._hps.train_actor:
            actor_loss_results = self._compute_actor_loss(
                module_id=module_id,
                dream_data=dream_data,
                value_targets=value_targets,
            )
        #if self._hps.use_curiosity:
        #    L_disagree = self._compute_disagree_loss(dream_data=dream_data)

        # Compile all the results to return.
        results = critic_loss_results.copy()
        results["VALUE_TARGETS_H_B"] = value_targets

        results["dream_data"] = dream_data
        #L_critic = results["CRITIC_L_total"]

        # Get the gradients from the tape.
        if self._hps.train_actor:
            results.update(actor_loss_results)
            #L_actor = results["ACTOR_L_total"]
        #if self._hps.use_curiosity:
        #    results["DISAGREE_L_total"] = L_disagree
        #    results["DISAGREE_intrinsic_rewards_H_B"] = (
        #        dream_data["rewards_intrinsic_t1_to_H_B"]
        #    )
        #    results["DISAGREE_intrinsic_rewards"] = tf.reduce_mean(
        #        dream_data["rewards_intrinsic_t1_to_H_B"]
        #    )

        results.update({
            # Forward train results.
            "WORLD_MODEL_forward_train_outs": fwd_out,
            "WORLD_MODEL_learned_initial_h": self.module[module_id].world_model.initial_h,

            # Prediction losses.
            # Decoder (obs) loss.
            "WORLD_MODEL_L_decoder_B_T": L_decoder_B_T,
            "WORLD_MODEL_L_decoder": L_decoder,
            # Reward loss.
            "WORLD_MODEL_L_reward_B_T": L_reward_two_hot_B_T,
            "WORLD_MODEL_L_reward": L_reward_two_hot,
            # Continue loss.
            "WORLD_MODEL_L_continue_B_T": L_continue_B_T,
            "WORLD_MODEL_L_continue": L_continue,
            # Total.
            "WORLD_MODEL_L_prediction_B_T": L_pred_B_T,
            "WORLD_MODEL_L_prediction": L_pred,

            # Dynamics loss.
            "WORLD_MODEL_L_dynamics_B_T": L_dyn_B_T,
            "WORLD_MODEL_L_dynamics": L_dyn,

            # Representation loss.
            "WORLD_MODEL_L_representation_B_T": L_rep_B_T,
            "WORLD_MODEL_L_representation": L_rep,

            # Total loss.
            "WORLD_MODEL_L_total_B_T": L_world_model_total_B_T,
            "WORLD_MODEL_L_total": L_world_model_total,

            #TODO: Move to grad stats.
            ## Gradient stats.
            #"WORLD_MODEL_gradients_maxabs": (
            #    tf.reduce_max([tf.reduce_max(tf.math.abs(g)) for g in gradients])
            #),
            #"WORLD_MODEL_gradients_clipped_by_glob_norm_maxabs": (
            #    tf.reduce_max([tf.reduce_max(tf.math.abs(g)) for g in clipped_gradients])
            #),
        })

        return results

    @override(TfLearner)
    def additional_update_per_module(
        self, module_id: ModuleID, sampled_kls: Dict[ModuleID, float], **kwargs
    ) -> Mapping[str, Any]:
        """Update the target networks and KL loss coefficients of each module.

        Args:

        """
        # Update EMA weights of the critic.
        self.module[module_id].dreamer_model.critic.update_ema()
        return {}

    def _compute_world_model_prediction_losses(
        self,
        *,
        rewards,
        continues,
        fwd_out,
    ):
        # Learn to produce symlog'd observation predictions.
        # If symlog is disabled (e.g. for uint8 image inputs), `obs_symlog_BxT` is the same
        # as `obs_BxT`.
        obs_BxT = fwd_out["sampled_obs_symlog_BxT"]
        obs_distr = fwd_out["obs_distribution_BxT"]
        # Fold time dim and flatten all other (image?) dims.
        obs_BxT = tf.reshape(
            obs_BxT, shape=[-1, int(np.prod(obs_BxT.shape.as_list()[1:]))]
        )

        # Neg logp loss.
        # decoder_loss = - obs_distr.log_prob(observations)
        # decoder_loss /= observations.shape.as_list()[1]
        # Squared diff loss w/ sum(!) over all (already folded) obs dims.
        decoder_loss_BxT = tf.reduce_sum(tf.math.square(obs_distr.loc - obs_BxT),
                                         axis=-1)

        # Unfold time rank back in.
        decoder_loss_B_T = tf.reshape(decoder_loss_BxT, (self._hps.batch_size_B, self._hps.batch_length_T))

        # The FiniteDiscrete reward bucket distribution computed by our reward predictor.
        # [B x num_buckets].
        reward_logits_BxT = fwd_out["reward_logits_BxT"]
        # Learn to produce symlog'd reward predictions.
        rewards = symlog(rewards)
        # Fold time dim.
        rewards = tf.reshape(rewards, shape=[-1])

        # A) Two-hot encode.
        two_hot_rewards = two_hot(rewards)
        # two_hot_rewards=[B*T, num_buckets]
        # predicted_reward_log_probs = tf.math.log(reward_distr.probs)
        # predicted_reward_probs = reward_distr.probs
        # predicted_reward_log_probs=[B*T, num_buckets]
        # reward_loss_two_hot = - tf.reduce_sum(tf.multiply(two_hot_rewards, predicted_reward_log_probs), axis=-1)
        # reward_loss_two_hot_BxT = - tf.math.log(tf.reduce_sum(tf.multiply(two_hot_rewards, predicted_reward_probs), axis=-1))
        reward_log_pred_BxT = reward_logits_BxT - tf.math.reduce_logsumexp(
            reward_logits_BxT, axis=-1, keepdims=True)
        # Multiply with two-hot targets and neg.
        reward_loss_two_hot_BxT = - tf.reduce_sum(reward_log_pred_BxT * two_hot_rewards,
                                                  axis=-1)
        # Unfold time rank back in.
        reward_loss_two_hot_B_T = tf.reshape(reward_loss_two_hot_BxT, (self._hps.batch_size_B, self._hps.batch_length_T))

        # B) Simple neg log(p) on distribution, NOT using two-hot.
        # reward_loss_logp_BxT = - reward_distr.log_prob(rewards)
        ## Unfold time rank back in.
        # reward_loss_logp_B_T = tf.reshape(reward_loss_logp_BxT, (self._hps.batch_size_B, self._hps.batch_length_T))

        # Probabilities that episode continues, computed by our continue predictor.
        # [B]
        continue_distr = fwd_out["continue_distribution_BxT"]
        # -log(p) loss
        # Fold time dim.
        continues = tf.reshape(continues, shape=[-1])
        continue_loss_BxT = - continue_distr.log_prob(continues)
        # Unfold time rank back in.
        continue_loss_B_T = tf.reshape(continue_loss_BxT, (self._hps.batch_size_B, self._hps.batch_length_T))

        return {
            "decoder_loss_B_T": decoder_loss_B_T,
            "reward_loss_two_hot_B_T": reward_loss_two_hot_B_T,
            # "reward_loss_logp_B_T": reward_loss_logp_B_T,
            "continue_loss_B_T": continue_loss_B_T,
            "prediction_loss_B_T": decoder_loss_B_T + reward_loss_two_hot_B_T + continue_loss_B_T,
        }

    def _compute_world_model_dynamics_and_representation_loss(self, *, fwd_out):
        # Actual distribution over stochastic internal states (z) produced by the encoder.
        z_posterior_probs_BxT = fwd_out["z_posterior_probs_BxT"]
        z_posterior_distr_BxT = tfp.distributions.Independent(
            tfp.distributions.OneHotCategorical(probs=z_posterior_probs_BxT),
            reinterpreted_batch_ndims=1,
        )

        # Actual distribution over stochastic internal states (z) produced by the
        # dynamics network.
        z_prior_probs_BxT = fwd_out["z_prior_probs_BxT"]
        z_prior_distr_BxT = tfp.distributions.Independent(
            tfp.distributions.OneHotCategorical(probs=z_prior_probs_BxT),
            reinterpreted_batch_ndims=1,
        )

        # Stop gradient for encoder's z-outputs:
        sg_z_posterior_distr_BxT = tfp.distributions.Independent(
            tfp.distributions.OneHotCategorical(
                probs=tf.stop_gradient(z_posterior_probs_BxT)
            ),
            reinterpreted_batch_ndims=1,
        )
        # Stop gradient for dynamics model's z-outputs:
        sg_z_prior_distr_BxT = tfp.distributions.Independent(
            tfp.distributions.OneHotCategorical(
                probs=tf.stop_gradient(z_prior_probs_BxT)
            ),
            reinterpreted_batch_ndims=1,
        )

        # Implement free bits. According to [1]:
        # "To avoid a degenerate solution where the dynamics are trivial to predict but
        # contain not enough information about the inputs, we employ free bits by clipping
        # the dynamics and representation losses below the value of 1 nat ≈ 1.44 bits. This
        # disables them while they are already minimized well to focus the world model
        # on its prediction loss"
        L_dyn_BxT = tf.math.maximum(
            1.0,
            ## Sum KL over all `num_categoricals` as these are independent.
            ## This is the same thing that a tfp.distributions.Independent() distribution
            ## with an underlying set of different Categoricals would do.
            # tf.reduce_sum(
            tfp.distributions.kl_divergence(sg_z_posterior_distr_BxT,
                                            z_prior_distr_BxT),
            #    axis=-1,
            # ),
        )
        # Unfold time rank back in.
        L_dyn_B_T = tf.reshape(L_dyn_BxT, (self._hps.batch_size_B, self._hps.batch_length_T))

        L_rep_BxT = tf.math.maximum(
            1.0,
            ## Sum KL over all `num_categoricals` as these are independent.
            ## This is the same thing that a tfp.distributions.Independent() distribution
            ## with an underlying set of different Categoricals would do.
            # tf.reduce_sum(
            tfp.distributions.kl_divergence(z_posterior_distr_BxT,
                                            sg_z_prior_distr_BxT),
            #    axis=-1,
            # ),
        )
        # Unfold time rank back in.
        L_rep_B_T = tf.reshape(L_rep_BxT, (self._hps.batch_size_B, self._hps.batch_length_T))

        return L_dyn_B_T, L_rep_B_T

    def _compute_actor_loss(
        self,
        *,
        module_id,
        dream_data,
        value_targets,
    ):
        actor = self.module[module_id].actor

        # Note: `value_targets` are NOT stop_gradient'd yet.
        scaled_value_targets_t0_to_Hm1_B = self._compute_scaled_value_targets(
            module_id=module_id,
            value_targets=value_targets,  # targets are already [t0 to H-1, B]
            value_predictions=dream_data["values_dreamed_t0_to_H_B"][:-1],
        )

        # Actions actually taken in the dream.
        actions_dreamed = tf.stop_gradient(dream_data["actions_dreamed_t0_to_H_B"])[:-1]
        dist_actions_t0_to_Hm1_B = dream_data["actions_dreamed_distributions_t0_to_H_B"][:-1]

        # Compute log(p)s of all possible actions in the dream.
        if isinstance(self.module[module_id].actor.action_space, gym.spaces.Discrete):
            # Note that when we create the Categorical action distributions, we compute
            # unimix probs, then math.log these and provide these log(p) as "logits" to the
            # Categorical. So here, we'll continue to work with log(p)s (not really "logits")!
            logp_actions_t0_to_Hm1_B = tf.stack(
                [dist.logits for dist in dist_actions_t0_to_Hm1_B],
                axis=0,
            )
            # Log probs of actions actually taken in the dream.
            logp_actions_dreamed_t0_to_Hm1_B = tf.reduce_sum(
                actions_dreamed * logp_actions_t0_to_Hm1_B,
                axis=-1,
            )
            # First term of loss function. [1] eq. 11.
            logp_loss_H_B = logp_actions_dreamed_t0_to_Hm1_B * tf.stop_gradient(
                scaled_value_targets_t0_to_Hm1_B
            )
        elif isinstance(actor.action_space, gym.spaces.Box):
            # TODO (Rohan138, Sven): Figure out how to vectorize this instead!
            logp_actions_dreamed_t0_to_Hm1_B = tf.stack([
                dist.log_prob(actions_dreamed[i])
                for i, dist in enumerate(dist_actions_t0_to_Hm1_B)
            ])
            # First term of loss function. [1] eq. 11.
            logp_loss_H_B = scaled_value_targets_t0_to_Hm1_B
        else:
            raise ValueError(f"Invalid action space: {actor.action_space}")

        assert len(logp_loss_H_B.shape) == 2

        # Add entropy loss term (second term [1] eq. 11).
        entropy_H_B = tf.stack(
            [dist.entropy()
             for dist in dream_data["actions_dreamed_distributions_t0_to_H_B"][:-1]
             ],
            axis=0,
        )
        assert len(entropy_H_B.shape) == 2
        entropy = tf.reduce_mean(entropy_H_B)

        L_actor_reinforce_term_H_B = - logp_loss_H_B
        L_actor_action_entropy_term_H_B = - self._hps.entropy_scale * entropy_H_B

        L_actor_H_B = L_actor_reinforce_term_H_B + L_actor_action_entropy_term_H_B
        # Mask out everything that goes beyond a predicted continue=False boundary.
        L_actor_H_B *= tf.stop_gradient(dream_data["dream_loss_weights_t0_to_H_B"])[:-1]
        L_actor = tf.reduce_mean(L_actor_H_B)

        return {
            "ACTOR_L_total_H_B": L_actor_H_B,
            "ACTOR_L_total": L_actor,
            "ACTOR_logp_actions_dreamed_H_B": logp_actions_dreamed_t0_to_Hm1_B,
            "ACTOR_scaled_value_targets_H_B": scaled_value_targets_t0_to_Hm1_B,
            "ACTOR_value_targets_pct95_ema": actor.ema_value_target_pct95,
            "ACTOR_value_targets_pct5_ema": actor.ema_value_target_pct5,
            "ACTOR_action_entropy_H_B": entropy_H_B,
            "ACTOR_action_entropy": entropy,

            # Individual loss terms.
            "ACTOR_L_neglogp_reinforce_term_H_B": L_actor_reinforce_term_H_B,
            "ACTOR_L_neglogp_reinforce_term": tf.reduce_mean(L_actor_reinforce_term_H_B),
            "ACTOR_L_neg_entropy_term_H_B": L_actor_action_entropy_term_H_B,
            "ACTOR_L_neg_entropy_term": tf.reduce_mean(L_actor_action_entropy_term_H_B),
        }

    def _compute_critic_loss(
        self,
        *,
        dream_data,
        value_targets,  # t0 to H-1, B
    ):
        H, B = dream_data["rewards_dreamed_t0_to_H_B"].shape[:2]
        Hm1 = H - 1
        # Note that value targets are NOT symlog'd and go from t0 to H-1, not H, like
        # all the other dream data.
        value_targets_t0_to_Hm1_B = tf.stop_gradient(value_targets)
        value_symlog_targets_t0_to_Hm1_B = symlog(value_targets_t0_to_Hm1_B)
        # Fold time rank (for two_hot'ing).
        value_symlog_targets_HxB = tf.reshape(value_symlog_targets_t0_to_Hm1_B, (-1,))
        value_symlog_targets_two_hot_HxB = two_hot(value_symlog_targets_HxB)
        # Unfold time rank.
        value_symlog_targets_two_hot_t0_to_Hm1_B = tf.reshape(
            value_symlog_targets_two_hot_HxB,
            shape=[Hm1, B, value_symlog_targets_two_hot_HxB.shape[-1]],
        )

        # Get (B x T x probs) tensor from return distributions.
        value_symlog_logits_HxB = dream_data["values_symlog_dreamed_logits_t0_to_HxB"]
        # Unfold time rank and cut last time index to match value targets.
        value_symlog_logits_t0_to_Hm1_B = tf.reshape(
            value_symlog_logits_HxB,
            shape=[H, B, value_symlog_logits_HxB.shape[-1]],
        )[:-1]

        values_log_pred_Hm1_B = value_symlog_logits_t0_to_Hm1_B - tf.math.reduce_logsumexp(value_symlog_logits_t0_to_Hm1_B, axis=-1, keepdims=True)
        # Multiply with two-hot targets and neg.
        value_loss_two_hot_H_B = - tf.reduce_sum(values_log_pred_Hm1_B * value_symlog_targets_two_hot_t0_to_Hm1_B, axis=-1)

        # Compute EMA regularization loss.
        # Expected values (dreamed) from the EMA (slow critic) net.
        # Note: Slow critic (EMA) outputs are already stop_gradient'd.
        value_symlog_ema_t0_to_Hm1_B = tf.stop_gradient(dream_data["v_symlog_dreamed_ema_t0_to_H_B"])[:-1]
        # Fold time rank (for two_hot'ing).
        value_symlog_ema_HxB = tf.reshape(value_symlog_ema_t0_to_Hm1_B, (-1,))
        value_symlog_ema_two_hot_HxB = two_hot(value_symlog_ema_HxB)
        # Unfold time rank.
        value_symlog_ema_two_hot_t0_to_Hm1_B = tf.reshape(
            value_symlog_ema_two_hot_HxB,
            shape=[Hm1, B, value_symlog_ema_two_hot_HxB.shape[-1]],
        )

        # Compute ema regularizer loss.
        # In the paper, it is not described how exactly to form this regularizer term and
        # how to weigh it.
        # So we follow Dani's repo here: `reg = -dist.log_prob(sg(self.slow(traj).mean()))`
        # with a weight of 1.0, where dist is the bucket'ized distribution output by the
        # fast critic. sg=stop gradient; mean() -> use the expected EMA values.
        # Multiply with two-hot targets and neg.
        ema_regularization_loss_H_B = - tf.reduce_sum(values_log_pred_Hm1_B * value_symlog_ema_two_hot_t0_to_Hm1_B, axis=-1)

        L_critic_H_B = (
            value_loss_two_hot_H_B + ema_regularization_loss_H_B
        )

        # Mask out everything that goes beyond a predicted continue=False boundary.
        L_critic_H_B *= tf.stop_gradient(dream_data["dream_loss_weights_t0_to_H_B"])[:-1]

        # Reduce over H- (time) axis (sum) and then B-axis (mean).
        L_critic = tf.reduce_mean(L_critic_H_B)

        return {
            # Symlog'd value targets. Critic learns to predict symlog'd values.
            "VALUE_TARGETS_symlog_H_B": value_symlog_targets_t0_to_Hm1_B,

            # Critic loss terms.
            "CRITIC_L_total": L_critic,
            "CRITIC_L_total_H_B": L_critic_H_B,
            "CRITIC_L_neg_logp_of_value_targets_H_B": value_loss_two_hot_H_B,
            "CRITIC_L_neg_logp_of_value_targets": tf.reduce_mean(value_loss_two_hot_H_B),
            "CRITIC_L_slow_critic_regularization_H_B": ema_regularization_loss_H_B,
            "CRITIC_L_slow_critic_regularization": tf.reduce_mean(ema_regularization_loss_H_B),
        }

    def _compute_value_targets(
        *,
        rewards_t0_to_H_BxT,
        intrinsic_rewards_t1_to_H_BxT,
        continues_t0_to_H_BxT,
        value_predictions_t0_to_H_BxT,
    ):
        """All args are (H, BxT, ...) and in non-symlog'd (real) space.

        Where H=1+horizon (start state + H steps dreamed), BxT=batch_size * batch_length
        (original trajectory time rank has been folded).

        Non-symlog is important b/c log(a+b) != log(a) + log(b).
        See [1] eq. 8 and 10.

        Thus, targets are always returned in real (non-symlog'd space).
        They need to be re-symlog'd before computing the critic loss from them (b/c the
        critic does produce predictions in symlog space).

        Rewards, continues, and value_predictions are all of shape [t0-H, B] (time-major),
        whereas returned targets are [t0 to H-1, B] (last timestep missing as target equals
        vf prediction in that location.
        """
        # The first reward is irrelevant (not used for any VF target).
        rewards_t1_to_H_BxT = rewards_t0_to_H_BxT[1:]
        if intrinsic_rewards_t1_to_H_BxT is not None:
            rewards_t1_to_H_BxT += intrinsic_rewards_t1_to_H_BxT

        # In all the following, when building value targets for t=1 to T=H,
        # exclude rewards & continues for t=1 b/c we don't need r1 or c1.
        # The target (R1) for V1 is built from r2, c2, and V2/R2.
        discount = continues_t0_to_H_BxT[1:] * self._hps.gamma  # shape=[2-16, BxT]
        Rs = [value_predictions_t0_to_H_BxT[-1]]  # Rs indices=[16]
        intermediates = rewards_t1_to_H_BxT + discount * (1 - self._hps.lambda_) * value_predictions_t0_to_H_BxT[1:]
        # intermediates.shape=[2-16, BxT]

        # Loop through reversed timesteps (axis=1) from T+1 to t=2.
        for t in reversed(range(len(discount))):
            Rs.append(intermediates[t] + discount[t] * self._hps.lambda_ * Rs[-1])

        # Reverse along time axis and cut the last entry (value estimate at very end cannot
        # be learnt from as it's the same as the ... well ... value estimate).
        targets = tf.stack(list(reversed(Rs))[:-1], axis=0)
        # targets.shape=[t0 to H-1,BxT]

        return targets

    def _compute_scaled_value_targets(
        self,
        *,
        module_id,
        value_targets,
        value_predictions,
    ):
        actor = self.module[module_id].actor

        value_targets_H_B = value_targets
        value_predictions_H_B = value_predictions

        # Compute S: [1] eq. 12.
        Per_R_5 = tfp.stats.percentile(value_targets_H_B, 5)
        Per_R_95 = tfp.stats.percentile(value_targets_H_B, 95)

        # Update EMAs stored in actor network.
        # Initial values: Just set.
        if tf.math.is_nan(actor.ema_value_target_pct5):
            actor.ema_value_target_pct5.assign(Per_R_5)
            actor.ema_value_target_pct95.assign(Per_R_95)
        # Later update (something already stored in EMA variable): Update EMA.
        else:
            actor.ema_value_target_pct5.assign(
                self._hps.return_normalization_decay * actor.ema_value_target_pct5 + (
                    1.0 - self._hps.return_normalization_decay
                ) * Per_R_5
            )
            actor.ema_value_target_pct95.assign(
                self._hps.return_normalization_decay * actor.ema_value_target_pct95 + (
                    1.0 - self._hps.return_normalization_decay
                ) * Per_R_95
            )

        # [1] eq. 11 (first term).
        # Dani's code: TODO: describe ...
        offset = actor.ema_value_target_pct5
        invscale = tf.math.maximum(1e-8, (actor.ema_value_target_pct95 - actor.ema_value_target_pct5))
        scaled_value_targets_H_B = (value_targets_H_B - offset) / invscale
        scaled_value_predictions_H_B = (value_predictions_H_B - offset) / invscale

        # Return advantages.
        return scaled_value_targets_H_B - scaled_value_predictions_H_B
