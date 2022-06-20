"""TensorFlow policy class used for DQN"""

from typing import Dict, List, Tuple, Type, Union

import ray
from ray.rllib.algorithms.dqn.utils import make_dqn_models, postprocess_nstep_and_prio
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import Categorical, TFActionDistribution
from ray.rllib.policy.dynamic_tf_policy_v2 import DynamicTFPolicyV2
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_mixins import (
    ComputeTDErrorMixin,
    LearningRateSchedule,
    TargetNetworkMixin,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import (
    huber_loss,
    minimize_and_clip,
    reduce_mean_ignore_inf,
)
from ray.rllib.utils.typing import (
    LocalOptimizer,
    ModelGradients,
    TensorType,
)

tf1, tf, tfv = try_import_tf()


# We need this builder function because we want to share the same
# custom logics between TF1 dynamic and TF2 eager policies.
def get_dqn_tf_policy(base: Type[Union[DynamicTFPolicyV2, EagerTFPolicyV2]]) -> Type:
    """Construct a SimpleQTFPolicy inheriting either dynamic or eager base policies.

    Args:
        base: Base class for this policy. DynamicTFPolicyV2 or EagerTFPolicyV2.

    Returns:
        A TF Policy to be used with MAMLTrainer.
    """

    class SimpleQTFPolicy(
        ComputeTDErrorMixin,
        TargetNetworkMixin,
        LearningRateSchedule,
        base,
    ):
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
                ray.rllib.algorithms.dqn.dqn.DQNConfig().to_dict(),
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

            LearningRateSchedule.__init__(self, config["lr"], config["lr_schedule"])
            ComputeTDErrorMixin.__init__(self)

            self.maybe_initialize_optimizer_and_loss()

            TargetNetworkMixin.__init__(self, obs_space, action_space, config)

        @override(base)
        def make_model(self) -> ModelV2:
            """Builds Q-model and target Q-model for Simple Q learning."""
            return make_dqn_models(self)

        @override(base)
        def optimizer(self) -> LocalOptimizer:
            if self.config["framework"] in ["tf2", "tfe"]:
                return tf.keras.optimizers.Adam(
                    learning_rate=self.cur_lr, epsilon=self.config["adam_epsilon"]
                )
            else:
                return tf1.train.AdamOptimizer(
                    learning_rate=self.cur_lr, epsilon=self.config["adam_epsilon"]
                )

        @override(base)
        def action_distribution_fn(
            self,
            model: ModelV2,
            input_dict: SampleBatch,
            *,
            explore: bool = True,
            **kwargs,
        ) -> Tuple[TensorType, type, List[TensorType]]:

            q_vals = self._compute_q_values(
                model, input_dict, state_batches=None, explore=explore
            )
            q_vals = q_vals[0] if isinstance(q_vals, tuple) else q_vals

            self.q_values = q_vals

            return self.q_values, Categorical, []  # state-out

        @override(base)
        def extra_action_out_fn(self) -> Dict[str, TensorType]:
            extra_action_fetches = super().extra_action_out_fn()
            extra_action_fetches.update({"q_values": self.q_values})
            return extra_action_fetches

        @override(base)
        def postprocess_trajectory(
            self, sample_batch, other_agent_batches=None, episode=None
        ):
            sample_batch = super().postprocess_trajectory(sample_batch)
            return postprocess_nstep_and_prio(self, sample_batch)

        @override(base)
        def loss(
            self,
            model: Union[ModelV2, "tf.keras.Model"],
            dist_class: Type[TFActionDistribution],
            train_batch: SampleBatch,
        ) -> Union[TensorType, List[TensorType]]:
            # q network evaluation
            q_t, q_logits_t, q_dist_t, _ = self._compute_q_values(
                model,
                SampleBatch({"obs": train_batch[SampleBatch.CUR_OBS]}),
                state_batches=None,
                explore=False,
            )

            # target q network evalution
            q_tp1, q_logits_tp1, q_dist_tp1, _ = self._compute_q_values(
                self.target_model,
                SampleBatch({"obs": train_batch[SampleBatch.NEXT_OBS]}),
                state_batches=None,
                explore=False,
            )
            if not hasattr(self, "target_q_func_vars"):
                self.target_q_func_vars = self.target_model.variables()

            # q scores for actions which we know were selected in the given state.
            one_hot_selection = tf.one_hot(
                tf.cast(train_batch[SampleBatch.ACTIONS], tf.int32), self.action_space.n
            )
            q_t_selected = tf.reduce_sum(q_t * one_hot_selection, 1)
            q_logits_t_selected = tf.reduce_sum(
                q_logits_t * tf.expand_dims(one_hot_selection, -1), 1
            )

            # compute estimate of best possible value starting from state at t + 1
            if self.config["double_q"]:
                (
                    q_tp1_using_online_net,
                    q_logits_tp1_using_online_net,
                    q_dist_tp1_using_online_net,
                    _,
                ) = self._compute_q_values(
                    model,
                    SampleBatch({"obs": train_batch[SampleBatch.NEXT_OBS]}),
                    state_batches=None,
                    explore=False,
                )
                q_tp1_best_using_online_net = tf.argmax(q_tp1_using_online_net, 1)
                q_tp1_best_one_hot_selection = tf.one_hot(
                    q_tp1_best_using_online_net, self.action_space.n
                )
                q_tp1_best = tf.reduce_sum(q_tp1 * q_tp1_best_one_hot_selection, 1)
                q_dist_tp1_best = tf.reduce_sum(
                    q_dist_tp1 * tf.expand_dims(q_tp1_best_one_hot_selection, -1), 1
                )
            else:
                q_tp1_best_one_hot_selection = tf.one_hot(
                    tf.argmax(q_tp1, 1), self.action_space.n
                )
                q_tp1_best = tf.reduce_sum(q_tp1 * q_tp1_best_one_hot_selection, 1)
                q_dist_tp1_best = tf.reduce_sum(
                    q_dist_tp1 * tf.expand_dims(q_tp1_best_one_hot_selection, -1), 1
                )

            if self.config["num_atoms"] > 1:
                # Distributional Q-learning which corresponds to an entropy loss

                z = tf.range(self.config["num_atoms"], dtype=tf.float32)
                z = self.config["v_min"] + z * (
                    self.config["v_max"] - self.config["v_min"]
                ) / float(self.config["num_atoms"] - 1)

                # (batch_size, 1) * (1, num_atoms) = (batch_size, num_atoms)
                r_tau = tf.expand_dims(
                    train_batch[SampleBatch.REWARDS], -1
                ) + self.config["gamma"] ** self.config["n_step"] * tf.expand_dims(
                    1.0 - tf.cast(train_batch[SampleBatch.DONES], tf.float32), -1
                ) * tf.expand_dims(
                    z, 0
                )
                r_tau = tf.clip_by_value(
                    r_tau, self.config["v_min"], self.config["v_max"]
                )
                b = (r_tau - self.config["v_min"]) / (
                    (self.config["v_max"] - self.config["v_min"])
                    / float(self.config["num_atoms"] - 1)
                )
                lb = tf.floor(b)
                ub = tf.math.ceil(b)
                # indispensable judgement which is missed in most implementations
                # when b happens to be an integer, lb == ub, so pr_j(s', a*) will
                # be discarded because (ub-b) == (b-lb) == 0
                floor_equal_ceil = tf.cast(tf.less(ub - lb, 0.5), tf.float32)

                l_project = tf.one_hot(
                    tf.cast(lb, dtype=tf.int32), self.config["num_atoms"]
                )  # (batch_size, num_atoms, num_atoms)
                u_project = tf.one_hot(
                    tf.cast(ub, dtype=tf.int32), self.config["num_atoms"]
                )  # (batch_size, num_atoms, num_atoms)
                ml_delta = q_dist_tp1_best * (ub - b + floor_equal_ceil)
                mu_delta = q_dist_tp1_best * (b - lb)
                ml_delta = tf.reduce_sum(
                    l_project * tf.expand_dims(ml_delta, -1), axis=1
                )
                mu_delta = tf.reduce_sum(
                    u_project * tf.expand_dims(mu_delta, -1), axis=1
                )
                m = ml_delta + mu_delta

                # Rainbow paper claims that using this cross entropy loss for
                # priority is robust and insensitive to `prioritized_replay_alpha`
                self._td_error = tf.nn.softmax_cross_entropy_with_logits(
                    labels=m, logits=q_logits_t_selected
                )
                self._q_loss = tf.reduce_mean(
                    self._td_error
                    * tf.cast(train_batch[SampleBatch.PRIO_WEIGHTS], tf.float32)
                )
                self._q_loss_stats = {
                    # TODO: better Q stats for dist dqn
                    "mean_td_error": tf.reduce_mean(self._td_error),
                    "q_loss": self._q_loss,
                }
            else:
                q_tp1_best_masked = (
                    1.0 - tf.cast(train_batch[SampleBatch.DONES], tf.float32)
                ) * q_tp1_best

                # compute RHS of bellman equation
                q_t_selected_target = (
                    train_batch[SampleBatch.REWARDS]
                    + self.config["gamma"] ** self.config["n_step"] * q_tp1_best_masked
                )

                # compute the error (potentially clipped)
                self._td_error = q_t_selected - tf.stop_gradient(q_t_selected_target)
                self._q_loss = tf.reduce_mean(
                    tf.cast(train_batch[SampleBatch.PRIO_WEIGHTS], tf.float32)
                    * huber_loss(self._td_error)
                )
                self._q_loss_stats = {
                    "mean_q": tf.reduce_mean(q_t_selected),
                    "min_q": tf.reduce_min(q_t_selected),
                    "max_q": tf.reduce_max(q_t_selected),
                    "mean_td_error": tf.reduce_mean(self._td_error),
                    "q_loss": self._q_loss,
                }

            return self._q_loss

        @override(base)
        def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
            return dict(
                {
                    "cur_lr": tf.cast(self.cur_lr, tf.float64),
                },
                **self._q_loss_stats,
            )

        @override(base)
        def compute_gradients_fn(
            self, optimizer: LocalOptimizer, loss: TensorType
        ) -> ModelGradients:
            if not hasattr(self, "q_func_vars"):
                self.q_func_vars = self.model.variables()

            return minimize_and_clip(
                optimizer,
                loss,
                var_list=self.q_func_vars,
                clip_val=self.config["grad_clip"],
            )

        @override(base)
        def extra_learn_fetches_fn(self) -> Dict[str, TensorType]:
            return {"td_error": self._td_error}

        def _compute_q_values(
            self,
            model: ModelV2,
            input_batch: SampleBatch,
            state_batches=None,
            seq_lens=None,
            explore=None,
            is_training: bool = False,
        ):

            model_out, state = model(input_batch, state_batches or [], seq_lens)

            if self.config["num_atoms"] > 1:
                (
                    action_scores,
                    z,
                    support_logits_per_action,
                    logits,
                    dist,
                ) = model.get_q_value_distributions(model_out)
            else:
                (action_scores, logits, dist) = model.get_q_value_distributions(
                    model_out
                )

            if self.config["dueling"]:
                state_score = model.get_state_value(model_out)
                if self.config["num_atoms"] > 1:
                    support_logits_per_action_mean = tf.reduce_mean(
                        support_logits_per_action, 1
                    )
                    support_logits_per_action_centered = (
                        support_logits_per_action
                        - tf.expand_dims(support_logits_per_action_mean, 1)
                    )
                    support_logits_per_action = (
                        tf.expand_dims(state_score, 1)
                        + support_logits_per_action_centered
                    )
                    support_prob_per_action = tf.nn.softmax(
                        logits=support_logits_per_action
                    )
                    value = tf.reduce_sum(
                        input_tensor=z * support_prob_per_action, axis=-1
                    )
                    logits = support_logits_per_action
                    dist = support_prob_per_action
                else:
                    action_scores_mean = reduce_mean_ignore_inf(action_scores, 1)
                    action_scores_centered = action_scores - tf.expand_dims(
                        action_scores_mean, 1
                    )
                    value = state_score + action_scores_centered
            else:
                value = action_scores

            return value, logits, dist, state

    return SimpleQTFPolicy


DQNTF1Policy = get_dqn_tf_policy(DynamicTFPolicyV2)
DQNTF2Policy = get_dqn_tf_policy(EagerTFPolicyV2)
