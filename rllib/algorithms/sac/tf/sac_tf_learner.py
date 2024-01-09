from typing import Mapping
from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.algorithms.sac.sac_learner import QF_PREDS, QF_TARGET_PREDS
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import TensorType

_, tf, _ = try_import_tf()


class SACTfLearner(TfLearner):
    """Implements loss and update logic for SAC."""

    @override(TfLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: SACConfig,
        batch: NestedDict,
        fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        deterministic = config["_deterministic_loss"]

        # TODO (simon): Add logic for continuous actions. We then need a
        # discriminating object for discrete vs. continuous as the module
        # cannot be accessed here.

        log_pis_t = tf.nn.log_softmax(fwd_out[SampleBatch.ACTION_DIST_INPUTS], -1)
        pis_t = tf.math.exp(log_pis_t)
        q_t = fwd_out[QF_PREDS]

        log_pis_tp1 = tf.nn.log_softmax(fwd_out["action_dist_inputs_next"], -1)
        pis_tp1 = tf.math.exp(log_pis_tp1)
        # TODO (simon): Add comment with reference.
        q_tp1 = fwd_out["qf_preds_next"] - config["alpha"] * log_pis_tp1

        one_hot_actions = tf.one_hot(batch[SampleBatch.ACTIONS], axis=-1)
        q_t_selected = tf.reduce_sum(q_t * one_hot_actions, axis=-1)

        q_tp1_best = tf.reduce_sum(tf.multiply(pis_tp1, q_tp1), axis=-1)
        # Consider only the non-terminated ones.
        q_tp1_best_masked = (
            1.0 - tf.cast(batch[SampleBatch.TERMINATEDS], tf.float32)
        ) * q_tp1_best

        # Compute RHS of Bellman equation for the Q-loss (critic(s), see. eq (8)
        # in https://arxiv.org/pdf/1801.01290.pdf)
        # TODO (simon): Add explanation for using Q instead of V.
        # TODO (simon): Check, if this works without casting rewards.
        q_t_selected_target = tf.stop_gradient(
            batch[SampleBatch.REWARDS]
            + config.gamma**config.n_step * q_tp1_best_masked
        )

        # Compute TD-error (potentially clipped).
        # TODO (simon) add here the twin-Q error.

        # Calculate critic loss(es).
        critic_loss = tf.reduce_mean(
            batch["prio_weights"]
            * tf.losses.Huber(delta=1.0)(q_t_selected, q_t_selected_target)
        )

        # TODO (simon): Add terms for continuous case.
        # TODO (simon): CHeck, if we need here also a possibly masked mean
        # if stateful encoders are used.
        alpha_loss = tf.reduce_mean(
            tf.reduce_sum(
                tf.multiply(
                    tf.stop_gradient(pis_t),
                    -tf.math.log(config.alpha)
                    * tf.stop_gradient(log_pis_t + config.target_entropy),
                ),
                axis=-1,
            )
        )

        actor_loss = tf.reduce_mean(
            tf.reduce_sum(
                tf.multiply(
                    # NOTE: We want to backpropagate through the policy network here.
                    pis_t,
                    # We do not want to backpropagate through the Q network here.
                    config.alpha * log_pis_t - tf.stop_gradient(q_t),
                ),
                axis=-1,
            )
        )

        total_loss = actor_loss + tf.math.add_n(critic_loss) + alpha_loss

        return total_loss
