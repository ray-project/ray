"""TensorFlow policy class used for Simple Q-Learning"""

import logging
from functools import partial
from typing import List, Tuple, Type

import gym
import ray
from ray.rllib.models import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import MultiCategorical2, TFActionDistribution
from ray.rllib.policy import Policy
from ray.rllib.policy.dynamic_tf_policy import DynamicTFPolicy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import huber_loss, make_tf_callable
from ray.rllib.utils.typing import TensorType, TrainerConfigDict

tf1, tf, tfv = try_import_tf()
logger = logging.getLogger(__name__)

BDQ_SCOPE = "bdq_func"
BDQ_TARGET_SCOPE = "target_bdq_func"


class TargetNetworkMixin:
    """Assign the `update_target` method to the SimpleQTFPolicy

    The function is called every `target_network_update_freq` steps by the
    master learner.
    """

    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: TrainerConfigDict,
    ):
        @make_tf_callable(self.get_session())
        def do_update():
            # update_target_fn will be called periodically to copy Q network to
            # target Q network
            update_target_expr = []
            assert len(self.bdq_func_vars) == len(self.target_bdq_func_vars), (
                self.bdq_func_vars,
                self.target_bdq_func_vars,
            )
            for var, var_target in zip(self.bdq_func_vars, self.target_bdq_func_vars):
                update_target_expr.append(var_target.assign(var))
                logger.debug("Update target op {}".format(var_target))
            return tf.group(*update_target_expr)

        self.update_target = do_update

    @property
    def bdq_func_vars(self):
        if not hasattr(self, "_bdq_func_vars"):
            self._bdq_func_vars = self.model.variables()
        return self._bdq_func_vars

    @property
    def target_bdq_func_vars(self):
        if not hasattr(self, "_target_bdq_func_vars"):
            self._target_bdq_func_vars = self.target_model.variables()
        return self._target_bdq_func_vars

    @override(TFPolicy)
    def variables(self):
        return self.bdq_func_vars + self.target_bdq_func_vars


def build_q_models(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> ModelV2:
    """Build q_model and target_model for Simple Q learning

    Note that this function works for both Tensorflow and PyTorch.

    Args:
        policy (Policy): The Policy, which will use the model for optimization.
        obs_space (gym.spaces.Space): The policy's observation space.
        action_space (gym.spaces.Space): The policy's action space.
        config (TrainerConfigDict):

    Returns:
        ModelV2: The Model for the Policy to use.
            Note: The target q model will not be returned, just assigned to
            `policy.target_model`.
    """
    if not isinstance(action_space, gym.spaces.MultiDiscrete):
        raise UnsupportedSpaceException(
            "Action space {} is not supported for BDQ.".format(action_space)
        )

    model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=action_space.n,
        model_config=config["model"],
        framework=config["framework"],
        name=BDQ_SCOPE,
    )

    policy.target_model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=action_space.n,
        model_config=config["model"],
        framework=config["framework"],
        name=BDQ_TARGET_SCOPE,
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
        partial(MultiCategorical2, **{"input_lens": q_vals.shape[1]}),
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
        policy (Policy): The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        dist_class (Type[ActionDistribution]): The action distribution class.
        train_batch (SampleBatch): The training data.

    Returns:
        TensorType: A single loss tensor.
    """
    action_per_branch = model.model_config["custom_model_config"]["action_per_branch"]
    num_action_branches = policy.action_space.n
    
    next_q_values = compute_q_values(
        policy, policy.model, train_batch[SampleBatch.NEXT_OBS], explore=False, is_training=False
    )
    argmax_a = tf.math.argmax(next_q_values, axis=-1)

    target_next = compute_q_values(
        policy, policy.target_model, train_batch[SampleBatch.NEXT_OBS], explore=False, is_training=False
    )
    
    if not hasattr(policy, "bdq_func_vars"):
        policy.bdq_func_vars = model.variables()
        policy.target_bdq_func_vars = policy.target_model.variables()

    argmax_a_mask = tf.one_hot(argmax_a, action_per_branch)
    max_next_q_vals = tf.multiply(target_next, argmax_a_mask)
    mean_max_next_q_vals = tf.reduce_mean(
        tf.reduce_sum(max_next_q_vals, axis=-1), axis=-1)

    dones = tf.cast(train_batch[SampleBatch.DONES], tf.float32)
    target_qvals = train_batch[SampleBatch.REWARDS] + mean_max_next_q_vals*policy.config["gamma"]*(1.0 - dones)
    target_qvals = tf.reshape(tf.repeat(target_qvals, num_action_branches * action_per_branch),
                                (-1, num_action_branches, action_per_branch))

    action_mask = tf.one_hot(tf.cast(train_batch[SampleBatch.ACTIONS], tf.int32), action_per_branch)
    target_qvals = tf.multiply(target_qvals, action_mask)

    # Train the model on the states and updated Q-values
    q_values = compute_q_values(
        policy, policy.model, train_batch[SampleBatch.CUR_OBS], explore=False
    )

    # Apply the masks to the Q-values to get the Q-value for action taken
    q_action = tf.multiply(q_values, action_mask)

    # Calculate loss between target Q-value and old Q-value
    td_error = tf.math.abs(target_qvals - q_action)
    loss = tf.reduce_mean(huber_loss(td_error))
        
    dims = tf.range(1, tf.rank(td_error))
    td_error = tf.reduce_mean(td_error, axis=dims)

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
        policy (Policy): The Policy object.
        obs_space (gym.spaces.Space): The Policy's observation space.
        action_space (gym.spaces.Space): The Policy's action space.
        config (TrainerConfigDict): The Policy's config.
    """
    TargetNetworkMixin.__init__(policy, obs_space, action_space, config)


# Build a child class of `DynamicTFPolicy`, given the custom functions defined
# above.
SimpleBDQTFPolicy: Type[DynamicTFPolicy] = build_tf_policy(
    name="SimpleBDQTFPolicy",
    get_default_config=lambda: ray.rllib.agents.bdq.bdq.BDQ_DEFAULT_CONFIG,
    make_model=build_q_models,
    action_distribution_fn=get_distribution_inputs_and_class,
    loss_fn=build_q_losses,
    extra_action_out_fn=lambda policy: {"q_values": policy.q_values},
    extra_learn_fetches_fn=lambda policy: {"td_error": policy.td_error},
    after_init=setup_late_mixins,
    mixins=[TargetNetworkMixin],
)
