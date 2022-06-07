"""TensorFlow policy class used for Simple Q-Learning"""

import logging
from typing import List, Tuple, Type

import gym
import ray
from ray.rllib.models import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import Categorical, TFActionDistribution
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy import Policy
from ray.rllib.policy.dynamic_tf_policy import DynamicTFPolicy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_mixins import TargetNetworkMixin
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import huber_loss
from ray.rllib.utils.typing import TensorType, TrainerConfigDict

tf1, tf, tfv = try_import_tf()
logger = logging.getLogger(__name__)

Q_SCOPE = "q_func"
Q_TARGET_SCOPE = "target_q_func"


def build_q_models(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> ModelV2:
    """Build q_model and target_model for Simple Q learning

    Note that this function works for both Tensorflow and PyTorch.

    Args:
        policy: The Policy, which will use the model for optimization.
        obs_space (gym.spaces.Space): The policy's observation space.
        action_space (gym.spaces.Space): The policy's action space.
        config (TrainerConfigDict):

    Returns:
        ModelV2: The Model for the Policy to use.
            Note: The target q model will not be returned, just assigned to
            `policy.target_model`.
    """
    if not isinstance(action_space, gym.spaces.Discrete):
        raise UnsupportedSpaceException(
            "Action space {} is not supported for DQN.".format(action_space)
        )

    model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=action_space.n,
        model_config=config["model"],
        framework=config["framework"],
        name=Q_SCOPE,
    )

    policy.target_model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=action_space.n,
        model_config=config["model"],
        framework=config["framework"],
        name=Q_TARGET_SCOPE,
    )

    return model


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


def build_q_losses(
    policy: Policy,
    model: ModelV2,
    dist_class: Type[TFActionDistribution],
    train_batch: SampleBatch,
) -> TensorType:
    """Constructs the loss for SimpleQTFPolicy.

    Args:
        policy: The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        dist_class (Type[ActionDistribution]): The action distribution class.
        train_batch: The training data.

    Returns:
        TensorType: A single loss tensor.
    """
    # q network evaluation
    q_t = compute_q_values(
        policy, policy.model, train_batch[SampleBatch.CUR_OBS], explore=False
    )

    # target q network evalution
    q_tp1 = compute_q_values(
        policy, policy.target_model, train_batch[SampleBatch.NEXT_OBS], explore=False
    )
    if not hasattr(policy, "q_func_vars"):
        policy.q_func_vars = model.variables()
        policy.target_q_func_vars = policy.target_model.variables()

    # q scores for actions which we know were selected in the given state.
    one_hot_selection = tf.one_hot(
        tf.cast(train_batch[SampleBatch.ACTIONS], tf.int32), policy.action_space.n
    )
    q_t_selected = tf.reduce_sum(q_t * one_hot_selection, 1)

    # compute estimate of best possible value starting from state at t + 1
    dones = tf.cast(train_batch[SampleBatch.DONES], tf.float32)
    q_tp1_best_one_hot_selection = tf.one_hot(
        tf.argmax(q_tp1, 1), policy.action_space.n
    )
    q_tp1_best = tf.reduce_sum(q_tp1 * q_tp1_best_one_hot_selection, 1)
    q_tp1_best_masked = (1.0 - dones) * q_tp1_best

    # compute RHS of bellman equation
    q_t_selected_target = (
        train_batch[SampleBatch.REWARDS] + policy.config["gamma"] * q_tp1_best_masked
    )

    # compute the error (potentially clipped)
    td_error = q_t_selected - tf.stop_gradient(q_t_selected_target)
    loss = tf.reduce_mean(huber_loss(td_error))

    # save TD error as an attribute for outside access
    policy.td_error = td_error

    return loss


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


def setup_late_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> None:
    """Call all mixin classes' constructors before SimpleQTFPolicy initialization.

    Args:
        policy: The Policy object.
        obs_space (gym.spaces.Space): The Policy's observation space.
        action_space (gym.spaces.Space): The Policy's action space.
        config: The Policy's config.
    """
    TargetNetworkMixin.__init__(policy, obs_space, action_space, config)


# Build a child class of `DynamicTFPolicy`, given the custom functions defined
# above.
SimpleQTFPolicy: Type[DynamicTFPolicy] = build_tf_policy(
    name="SimpleQTFPolicy",
    get_default_config=lambda: ray.rllib.algorithms.simple_q.simple_q.DEFAULT_CONFIG,
    make_model=build_q_models,
    action_distribution_fn=get_distribution_inputs_and_class,
    loss_fn=build_q_losses,
    extra_action_out_fn=lambda policy: {"q_values": policy.q_values},
    extra_learn_fetches_fn=lambda policy: {"td_error": policy.td_error},
    after_init=setup_late_mixins,
    mixins=[TargetNetworkMixin],
)
