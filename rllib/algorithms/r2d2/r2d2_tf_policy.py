"""TensorFlow policy class used for R2D2."""

from typing import Dict, List, Type, Union

import ray
from ray.rllib.algorithms.dqn.dqn_tf_policy import DQNTF1Policy, DQNTF2Policy
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.tf_utils import huber_loss
from ray.rllib.utils.typing import TensorType, TFPolicyV2Type

tf1, tf, tfv = try_import_tf()


# We need this builder function because we want to share the same
# custom logics between TF1 dynamic and TF2 eager policies.
def get_r2d2_tf_policy(base: TFPolicyV2Type) -> TFPolicyV2Type:
    """Construct a R2D2TFPolicy inheriting either dynamic or eager base policies.

    Args:
        base: Base class for this policy. DynamicTFPolicyV2 or EagerTFPolicyV2.

    Returns:
        A TF Policy to be used with MAMLTrainer.
    """

    class R2D2TFPolicy(base):
        def __init__(
            self,
            obs_space,
            action_space,
            config,
            existing_model=None,
            existing_inputs=None,
        ):
            # First thing first, enable eager execution if necessary.
            base.enable_eager_execution_if_necessary()

            config = dict(
                ray.rllib.algorithms.r2d2.r2d2.R2D2Config().to_dict(),
                **config,
            )

            # Initialize base class.
            base.__init__(
                self,
                obs_space,
                action_space,
                config,
                existing_inputs=existing_inputs,
                existing_model=existing_model,
            )

        @override(base)
        def make_model(self) -> ModelV2:
            """Builds Q-model and target Q-model for Simple Q learning."""
            # Create the policy's models.
            model = super().make_model()

            # Assert correct model type by checking the init state to be present.
            # For attention nets: These don't necessarily publish their init state via
            # Model.get_initial_state, but may only use the trajectory view API
            # (view_requirements).
            assert (
                model.get_initial_state() != []
                or model.view_requirements.get("state_in_0") is not None
            ), (
                "R2D2 requires its model to be a recurrent one! Try using "
                "`model.use_lstm` or `model.use_attention` in your config "
                "to auto-wrap your model with an LSTM- or attention net."
            )

            return model

        @override(base)
        def loss(
            self,
            model: Union[ModelV2, "tf.keras.Model"],
            dist_class: Type[TFActionDistribution],
            train_batch: SampleBatch,
        ) -> Union[TensorType, List[TensorType]]:
            config = self.config

            # Construct internal state inputs.
            i = 0
            state_batches = []
            while "state_in_{}".format(i) in train_batch:
                state_batches.append(train_batch["state_in_{}".format(i)])
                i += 1
            assert state_batches

            # Q-network evaluation (at t).
            q, _, _, _ = self._compute_q_values(
                model,
                train_batch,
                state_batches=state_batches,
                seq_lens=train_batch.get(SampleBatch.SEQ_LENS),
            )

            # Target Q-network evaluation (at t+1).
            q_target, _, _, _ = self._compute_q_values(
                self.target_model,
                train_batch,
                state_batches=state_batches,
                seq_lens=train_batch.get(SampleBatch.SEQ_LENS),
            )

            if not hasattr(self, "target_q_func_vars"):
                self.target_q_func_vars = self.target_model.variables()

            actions = tf.cast(train_batch[SampleBatch.ACTIONS], tf.int64)
            dones = tf.cast(train_batch[SampleBatch.DONES], tf.float32)
            rewards = train_batch[SampleBatch.REWARDS]
            weights = tf.cast(train_batch[SampleBatch.PRIO_WEIGHTS], tf.float32)

            B = tf.shape(state_batches[0])[0]
            T = tf.shape(q)[0] // B

            # Q scores for actions which we know were selected in the given state.
            one_hot_selection = tf.one_hot(actions, self.action_space.n)
            q_selected = tf.reduce_sum(
                tf.where(q > tf.float32.min, q, tf.zeros_like(q)) * one_hot_selection,
                axis=1
            )

            if config["double_q"]:
                best_actions = tf.argmax(q, axis=1)
            else:
                best_actions = tf.argmax(q_target, axis=1)

            best_actions_one_hot = tf.one_hot(best_actions, self.action_space.n)
            q_target_best = tf.reduce_sum(
                tf.where(q_target > tf.float32.min, q_target, tf.zeros_like(q_target))
                * best_actions_one_hot,
                axis=1,
            )

            if config["num_atoms"] > 1:
                raise ValueError("Distributional R2D2 not supported yet!")
            else:
                q_target_best_masked_tp1 = (1.0 - dones) * tf.concat(
                    [q_target_best[1:], tf.constant([0.0])], axis=0
                )

                if config["use_h_function"]:
                    h_inv = self._h_inverse(q_target_best_masked_tp1,
                                      config["h_function_epsilon"])
                    target = self._h_function(
                        rewards + config["gamma"] ** config["n_step"] * h_inv,
                        config["h_function_epsilon"],
                    )
                else:
                    target = (
                        rewards + config["gamma"] ** config[
                        "n_step"] * q_target_best_masked_tp1
                    )

                # Seq-mask all loss-related terms.
                seq_mask = tf.sequence_mask(train_batch[SampleBatch.SEQ_LENS], T)[:,
                           :-1]
                # Mask away also the burn-in sequence at the beginning.
                burn_in = self.config["replay_buffer_config"]["replay_burn_in"]
                # Making sure, this works for both static graph and eager.
                if burn_in > 0:
                    seq_mask = tf.cond(
                        pred=tf.convert_to_tensor(burn_in, tf.int32) < T,
                        true_fn=lambda: tf.concat(
                            [tf.fill([B, burn_in], False), seq_mask[:, burn_in:]], 1
                        ),
                        false_fn=lambda: seq_mask,
                    )

                def reduce_mean_valid(t):
                    return tf.reduce_mean(tf.boolean_mask(t, seq_mask))

                # Make sure to use the correct time indices:
                # Q(t) - [gamma * r + Q^(t+1)]
                q_selected = tf.reshape(q_selected, [B, T])[:, :-1]
                td_error = q_selected - tf.stop_gradient(
                    tf.reshape(target, [B, T])[:, :-1])
                td_error = td_error * tf.cast(seq_mask, tf.float32)
                weights = tf.reshape(weights, [B, T])[:, :-1]
                self._total_loss = reduce_mean_valid(weights * huber_loss(td_error))
                # Store the TD-error per time chunk (b/c we need only one mean
                # prioritized replay weight per stored sequence).
                self._td_error = tf.reduce_mean(td_error, axis=-1)
                self._loss_stats = {
                    "mean_q": reduce_mean_valid(q_selected),
                    "min_q": tf.reduce_min(q_selected),
                    "max_q": tf.reduce_max(q_selected),
                    "mean_td_error": reduce_mean_valid(td_error),
                }

            return self._total_loss

        @override(base)
        def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
            return dict(
                {
                    "cur_lr": self.cur_lr,
                },
                **self._loss_stats
            )

        def _h_function(self, x, epsilon=1.0):
            """h-function to normalize target Qs, described in the paper [1].

            h(x) = sign(x) * [sqrt(abs(x) + 1) - 1] + epsilon * x

            Used in [1] in combination with h_inverse:
              targets = h(r + gamma * h_inverse(Q^))
            """
            return tf.sign(x) * (tf.sqrt(tf.abs(x) + 1.0) - 1.0) + epsilon * x

        def _h_inverse(self, x, epsilon=1.0):
            """Inverse if the above h-function, described in the paper [1].

            If x > 0.0:
            h-1(x) = [2eps * x + (2eps + 1) - sqrt(4eps x + (2eps + 1)^2)] /
                (2 * eps^2)

            If x < 0.0:
            h-1(x) = [2eps * x + (2eps + 1) + sqrt(-4eps x + (2eps + 1)^2)] /
                (2 * eps^2)
            """
            two_epsilon = epsilon * 2
            if_x_pos = (
                           two_epsilon * x
                           + (two_epsilon + 1.0)
                           - tf.sqrt(4.0 * epsilon * x + (two_epsilon + 1.0) ** 2)
                       ) / (2.0 * epsilon ** 2)
            if_x_neg = (
                           two_epsilon * x
                           - (two_epsilon + 1.0)
                           + tf.sqrt(-4.0 * epsilon * x + (two_epsilon + 1.0) ** 2)
                       ) / (2.0 * epsilon ** 2)
            return tf.where(x < 0.0, if_x_neg, if_x_pos)

    return R2D2TFPolicy


R2D2TF1Policy = get_r2d2_tf_policy(DQNTF1Policy)
R2D2TF2Policy = get_r2d2_tf_policy(DQNTF2Policy)
