"""TensorFlow policy class used for Simple Q-Learning"""

import logging
from typing import List, Tuple, Type, Union

import gym
import ray
from ray.rllib.models import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import Categorical, TFActionDistribution
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy import Policy
from ray.rllib.policy.dynamic_tf_policy_v2 import DynamicTFPolicyV2
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_mixins import TargetNetworkMixin
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import huber_loss
from ray.rllib.utils.typing import TensorType, TrainerConfigDict

tf1, tf, tfv = try_import_tf()
logger = logging.getLogger(__name__)

Q_SCOPE = "q_func"
Q_TARGET_SCOPE = "target_q_func"



def get_distribution_inputs_and_class(
    policy: Policy,
    q_model: ModelV2,
    obs_batch: TensorType,
    *,
    explore=True,
    is_training=True,
    **kwargs
) -> Tuple[TensorType, type, List[TensorType]]:
    """Build the action distribution"""
    q_vals = compute_q_values(policy, q_model, obs_batch, explore, is_training)
    q_vals = q_vals[0] if isinstance(q_vals, tuple) else q_vals

    policy.q_values = q_vals
    return (
        policy.q_values,
        (TorchCategorical if policy.config["framework"] == "torch" else Categorical),
        [],
    )  # state-outs



def compute_q_values(
    policy: Policy, model: ModelV2, obs: TensorType, explore, is_training=None
) -> TensorType:
    _is_training = (
        is_training
        if is_training is not None
        else policy._get_is_training_placeholder()
    )
    model_out, _ = model(SampleBatch(obs=obs, _is_training=_is_training), [], None)

    return model_out





# Build a child class of `DynamicTFPolicy`, given the custom functions defined
# above.
#SimpleQTFPolicy: Type[DynamicTFPolicy] = build_tf_policy(
#    action_distribution_fn=get_distribution_inputs_and_class,
#    extra_action_out_fn=lambda policy: {"q_values": policy.q_values},
#    extra_learn_fetches_fn=lambda policy: {"td_error": policy.td_error},
#)


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

    class SimpleQTFPolicy(
        ComputeAndClipGradsMixIn, base
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
                ray.rllib.algorithms.dqn.simple_q.SimpleQConfig().to_dict(),
                **config,
            )
            #validate_config(self, obs_space, action_space, config)

            # Initialize base class.
            base.__init__(
                self,
                obs_space,
                action_space,
                config,
                existing_inputs=existing_inputs,
                existing_model=existing_model,
            )

            ComputeAndClipGradsMixIn.__init__(self)

            # Note: this is a bit ugly, but loss and optimizer initialization must
            # happen after all the MixIns are initialized.
            self.maybe_initialize_optimizer_and_loss()

            TargetNetworkMixin.__init__(self, obs_space, action_space, config)

        @override(base)
        def make_model(self) -> ModelV2:
            """Build q_model and target_model for Simple Q learning

            Note that this function works for both Tensorflow and PyTorch.

            Returns:
                ModelV2: The Model for the Policy to use.
                Note: The target q model will not be returned, just assigned to
                `policy.target_model`.
            """
            if not isinstance(self.action_space, gym.spaces.Discrete):
                raise UnsupportedSpaceException(
                    f"Action space {self.action_space} is not supported for DQN."
                )

            model = ModelCatalog.get_model_v2(
                obs_space=self.observation_space,
                action_space=self.action_space,
                num_outputs=self.action_space.n,
                model_config=self.config["model"],
                framework=self.config["framework"],
                name=Q_SCOPE,
            )

            self.target_model = ModelCatalog.get_model_v2(
                obs_space=self.observation_space,
                action_space=self.action_space,
                num_outputs=self.action_space.n,
                model_config=self.config["model"],
                framework=self.config["framework"],
                name=Q_TARGET_SCOPE,
            )

            return model

        @override(base)
        def loss(
            self,
            model: Union[ModelV2, "tf.keras.Model"],
            dist_class: Type[TFActionDistribution],
            train_batch: SampleBatch,
        ) -> Union[TensorType, List[TensorType]]:
            # q network evaluation
            q_t = compute_q_values(
                self, self.model, train_batch[SampleBatch.CUR_OBS], explore=False
            )

            # target q network evalution
            q_tp1 = compute_q_values(
                self, self.target_model, train_batch[SampleBatch.NEXT_OBS],
                explore=False
            )
            if not hasattr(self, "q_func_vars"):
                self.q_func_vars = model.variables()
                self.target_q_func_vars = self.target_model.variables()

            # q scores for actions which we know were selected in the given state.
            one_hot_selection = tf.one_hot(
                tf.cast(train_batch[SampleBatch.ACTIONS], tf.int32),
                self.action_space.n
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
                train_batch[SampleBatch.REWARDS] + self.config[
                "gamma"] * q_tp1_best_masked
            )

            # compute the error (potentially clipped)
            td_error = q_t_selected - tf.stop_gradient(q_t_selected_target)
            loss = tf.reduce_mean(huber_loss(td_error))

            # save TD error as an attribute for outside access
            self.td_error = td_error

            return loss

    return SimpleQTFPolicy


SimpleQDynamicTFPolicy = get_simple_q_tf_policy(DynamicTFPolicyV2)
SimpleQEagerTFPolicy = get_simple_q_tf_policy(EagerTFPolicyV2)
