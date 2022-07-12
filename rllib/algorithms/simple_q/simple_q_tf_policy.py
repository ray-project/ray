"""TensorFlow policy class used for Simple Q-Learning"""

import logging
from typing import Dict, List, Tuple, Type, Union

import ray
from ray.rllib.algorithms.simple_q.utils import make_q_models
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import Categorical, TFActionDistribution
from ray.rllib.policy.dynamic_tf_policy_v2 import DynamicTFPolicyV2
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_mixins import TargetNetworkMixin, compute_gradients
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import huber_loss
from ray.rllib.utils.typing import (
    LocalOptimizer,
    ModelGradients,
    TensorStructType,
    TensorType,
)

tf1, tf, tfv = try_import_tf()
logger = logging.getLogger(__name__)


# We need this builder function because we want to share the same
# custom logics between TF1 dynamic and TF2 eager policies.
def get_simple_q_tf_policy(
    base: Type[Union[DynamicTFPolicyV2, EagerTFPolicyV2]]
) -> Type:
    """Construct a SimpleQTFPolicy inheriting either dynamic or eager base policies.

    Args:
        base: Base class for this policy. DynamicTFPolicyV2 or EagerTFPolicyV2.

    Returns:
        A TF Policy to be used with MAMLTrainer.
    """

    class SimpleQTFPolicy(TargetNetworkMixin, base):
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
                ray.rllib.algorithms.simple_q.simple_q.SimpleQConfig().to_dict(),
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

            # Note: this is a bit ugly, but loss and optimizer initialization must
            # happen after all the MixIns are initialized.
            self.maybe_initialize_optimizer_and_loss()

            TargetNetworkMixin.__init__(self, obs_space, action_space, config)

        @override(base)
        def make_model(self) -> ModelV2:
            """Builds Q-model and target Q-model for Simple Q learning."""
            model, self.target_model = make_q_models(self)
            return model

        @override(base)
        def action_distribution_fn(
            self,
            model: ModelV2,
            *,
            obs_batch: TensorType,
            state_batches: TensorType,
            **kwargs,
        ) -> Tuple[TensorType, type, List[TensorType]]:
            # Compute the Q-values for each possible action, using our Q-value network.
            q_vals = self._compute_q_values(self.model, obs_batch, is_training=False)
            return q_vals, Categorical, state_batches

        def xyz_compute_actions(
            self,
            *,
            input_dict,
            explore=True,
            timestep=None,
            episodes=None,
            is_training=False,
            **kwargs,
        ) -> Tuple[TensorStructType, List[TensorType], Dict[str, TensorStructType]]:
            if timestep is None:
                timestep = self.global_timestep
            # Compute the Q-values for each possible action, using our Q-value network.
            q_vals = self._compute_q_values(
                self.model, input_dict[SampleBatch.OBS], is_training=is_training
            )
            # Use a Categorical distribution for the exploration component.
            # This way, it may either sample storchastically (e.g. when using SoftQ)
            # or deterministically/greedily (e.g. when using EpsilonGreedy).
            distribution = Categorical(q_vals, self.model)
            # Call the exploration component's `get_exploration_action` method to
            # explore, if necessary.
            actions, logp = self.exploration.get_exploration_action(
                action_distribution=distribution, timestep=timestep, explore=explore
            )
            # Return (exploration) actions, state_outs (empty list), and extra outs.
            return (
                actions,
                [],
                {
                    "q_values": q_vals,
                    SampleBatch.ACTION_LOGP: logp,
                    SampleBatch.ACTION_PROB: tf.exp(logp),
                    SampleBatch.ACTION_DIST_INPUTS: q_vals,
                },
            )

        @override(base)
        def loss(
            self,
            model: Union[ModelV2, "tf.keras.Model"],
            dist_class: Type[TFActionDistribution],
            train_batch: SampleBatch,
        ) -> Union[TensorType, List[TensorType]]:
            # q network evaluation
            q_t = self._compute_q_values(self.model, train_batch[SampleBatch.CUR_OBS])

            # target q network evalution
            q_tp1 = self._compute_q_values(
                self.target_model,
                train_batch[SampleBatch.NEXT_OBS],
            )
            if not hasattr(self, "q_func_vars"):
                self.q_func_vars = model.variables()
                self.target_q_func_vars = self.target_model.variables()

            # q scores for actions which we know were selected in the given state.
            one_hot_selection = tf.one_hot(
                tf.cast(train_batch[SampleBatch.ACTIONS], tf.int32), self.action_space.n
            )
            q_t_selected = tf.reduce_sum(q_t * one_hot_selection, 1)

            # compute estimate of best possible value starting from state at t + 1
            dones = tf.cast(train_batch[SampleBatch.DONES], tf.float32)
            q_tp1_best_one_hot_selection = tf.one_hot(
                tf.argmax(q_tp1, 1), self.action_space.n
            )
            q_tp1_best = tf.reduce_sum(q_tp1 * q_tp1_best_one_hot_selection, 1)
            q_tp1_best_masked = (1.0 - dones) * q_tp1_best

            # compute RHS of bellman equation
            q_t_selected_target = (
                train_batch[SampleBatch.REWARDS]
                + self.config["gamma"] * q_tp1_best_masked
            )

            # compute the error (potentially clipped)
            td_error = q_t_selected - tf.stop_gradient(q_t_selected_target)
            loss = tf.reduce_mean(huber_loss(td_error))

            # save TD error as an attribute for outside access
            self.td_error = td_error

            return loss

        @override(base)
        def compute_gradients_fn(
            self, optimizer: LocalOptimizer, loss: TensorType
        ) -> ModelGradients:
            return compute_gradients(self, optimizer, loss)

        @override(base)
        def extra_learn_fetches_fn(self) -> Dict[str, TensorType]:
            return {"td_error": self.td_error}

        def _compute_q_values(
            self, model: ModelV2, obs_batch: TensorType, is_training=None
        ) -> TensorType:
            _is_training = (
                is_training
                if is_training is not None
                else self._get_is_training_placeholder()
            )
            model_out, _ = model(
                SampleBatch(obs=obs_batch, _is_training=_is_training), [], None
            )

            return model_out

    return SimpleQTFPolicy


SimpleQTF1Policy = get_simple_q_tf_policy(DynamicTFPolicyV2)
SimpleQTF2Policy = get_simple_q_tf_policy(EagerTFPolicyV2)
