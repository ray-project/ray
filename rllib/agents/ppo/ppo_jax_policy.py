"""
JAX policy class used for PPO.
"""
import gym
import logging
from typing import List, Type, Union

import ray
from ray.rllib.agents.a3c.a3c_torch_policy import apply_grad_clipping
from ray.rllib.agents.ppo.ppo_tf_policy import postprocess_ppo_gae, \
    setup_config
from ray.rllib.agents.ppo.ppo_torch_policy import vf_preds_fetches, \
    KLCoeffMixin, kl_and_loss_stats
from ray.rllib.evaluation.postprocessing import compute_gae_for_sample_batch, \
    Postprocessing
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.policy.jax_policy import LearningRateSchedule
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy import EntropyCoeffSchedule
from ray.rllib.utils.framework import try_import_jax
from ray.rllib.utils.jax_ops import explained_variance
from ray.rllib.utils.typing import TensorType, TrainerConfigDict

jax, flax = try_import_jax()
jnp = None
if jax:
    import jax.numpy as jnp

logger = logging.getLogger(__name__)


def ppo_surrogate_loss(
        policy: Policy,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
        vars=None,
) -> Union[TensorType, List[TensorType]]:
    """Constructs the loss for Proximal Policy Objective.

    Args:
        policy (Policy): The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        dist_class (Type[ActionDistribution]: The action distr. class.
        train_batch (SampleBatch): The training data.

    Returns:
        Union[TensorType, List[TensorType]]: A single loss tensor or a list
            of loss tensors.
    """
    def loss_(train_batch, vars=None):
        #if vars:
        #    for k, v in vars.items():
        #        setattr(model, k, v)

        logits, value_out, state = model.forward_(train_batch["obs"])
        curr_action_dist = dist_class(logits, None)

        prev_action_dist = dist_class(
            train_batch[SampleBatch.ACTION_DIST_INPUTS], None)

        logp_ratio = jnp.exp(
            curr_action_dist.logp(train_batch[SampleBatch.ACTIONS]) -
            train_batch[SampleBatch.ACTION_LOGP])
        action_kl = prev_action_dist.kl(curr_action_dist)
        policy._mean_kl = jnp.mean(action_kl)

        curr_entropy = curr_action_dist.entropy()
        policy._mean_entropy = jnp.mean(curr_entropy)

        surrogate_loss = jnp.minimum(
            train_batch[Postprocessing.ADVANTAGES] * logp_ratio,
            train_batch[Postprocessing.ADVANTAGES] * jnp.clip(
                logp_ratio, 1 - policy.config["clip_param"],
                1 + policy.config["clip_param"]))
        policy._mean_policy_loss = jnp.mean(-surrogate_loss)

        if policy.config["use_gae"]:
            prev_value_fn_out = train_batch[SampleBatch.VF_PREDS]
            #value_fn_out = model.value_function()
            vf_loss1 = jnp.square(value_out -
                                  train_batch[Postprocessing.VALUE_TARGETS])
            vf_clipped = prev_value_fn_out + jnp.clip(
                value_out - prev_value_fn_out, -policy.config["vf_clip_param"],
                policy.config["vf_clip_param"])
            vf_loss2 = jnp.square(vf_clipped -
                                  train_batch[Postprocessing.VALUE_TARGETS])
            vf_loss = jnp.maximum(vf_loss1, vf_loss2)
            policy._mean_vf_loss = jnp.mean(vf_loss)
            total_loss = jnp.mean(-surrogate_loss + policy.kl_coeff * action_kl +
                                  policy.config["vf_loss_coeff"] * vf_loss -
                                  policy.entropy_coeff * curr_entropy)
        else:
            policy._mean_vf_loss = 0.0
            total_loss = jnp.mean(-surrogate_loss + policy.kl_coeff * action_kl -
                                  policy.entropy_coeff * curr_entropy)
        #policy._value_out = value_out
        #policy._total_loss = total_loss

        #policy._vf_explained_var = explained_variance(
        #    train_batch[Postprocessing.VALUE_TARGETS],
        #    value_out)  # policy.model.value_function()

        #if vars:
        #    policy._total_loss = policy._total_loss.primal
        #    policy._mean_policy_loss = policy._mean_policy_loss.primal
        #    policy._mean_vf_loss = policy._mean_vf_loss.primal
        #    policy._vf_explained_var = policy._vf_explained_var.primal
        #    policy._mean_entropy = policy._mean_entropy.primal
        #    policy._mean_kl = policy._mean_kl.primal

        return total_loss #, mean_policy_loss, mean_vf_loss, value_out, mean_entropy, mean_kl

    if not hasattr(policy, "jit_loss"):
        policy.jit_loss = jax.jit(loss_)
        policy.gradient_loss = jax.grad(policy.jit_loss, argnums=1)# 4

    #policy._total_loss = policy.jit_loss(train_batch["obs"], vars)

    # Store stats in policy for stats_fn.
    #policy._total_loss = total_loss
    #policy._mean_policy_loss = mean_policy_loss
    #policy._mean_vf_loss = mean_vf_loss
    #policy._vf_explained_var = explained_variance(
    #    train_batch[Postprocessing.VALUE_TARGETS],
    #    policy._value_out) #policy.model.value_function()
    #policy._mean_entropy = mean_entropy
    #policy._mean_kl = mean_kl

    ret = policy.gradient_loss({k: train_batch[k] for k, v in train_batch.items()}, vars)


class ValueNetworkMixin:
    """Assigns the `_value()` method to the PPOPolicy.

    This way, Policy can call `_value()` to get the current VF estimate on a
    single(!) observation (as done in `postprocess_trajectory_fn`).
    Note: When doing this, an actual forward pass is being performed.
    This is different from only calling `model.value_function()`, where
    the result of the most recent forward pass is being used to return an
    already calculated tensor.
    """

    def __init__(self, obs_space, action_space, config):
        # When doing GAE, we need the value function estimate on the
        # observation.
        if config["use_gae"]:

            # Input dict is provided to us automatically via the Model's
            # requirements. It's a single-timestep (last one in trajectory)
            # input_dict.
            assert config["_use_trajectory_view_api"]

            def value(**input_dict):
                _, value_out, _ = self.model.from_batch(
                    input_dict, is_training=False)
                # [0] = remove the batch dim.
                return value_out[0]

        # When not doing GAE, we do not require the value function's output.
        else:

            def value(*args, **kwargs):
                return 0.0

        self._value = value


def setup_mixins(policy: Policy, obs_space: gym.spaces.Space,
                 action_space: gym.spaces.Space,
                 config: TrainerConfigDict) -> None:
    """Call all mixin classes' constructors before PPOPolicy initialization.

    Args:
        policy (Policy): The Policy object.
        obs_space (gym.spaces.Space): The Policy's observation space.
        action_space (gym.spaces.Space): The Policy's action space.
        config (TrainerConfigDict): The Policy's config.
    """
    ValueNetworkMixin.__init__(policy, obs_space, action_space, config)
    KLCoeffMixin.__init__(policy, config)
    EntropyCoeffSchedule.__init__(policy, config["entropy_coeff"],
                                  config["entropy_coeff_schedule"])
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])


# Build a child class of `JAXPolicy`, given the custom functions defined
# above.
PPOJAXPolicy = build_policy_class(
    name="PPOJAXPolicy",
    framework="jax",
    get_default_config=lambda: ray.rllib.agents.ppo.ppo.DEFAULT_CONFIG,
    loss_fn=ppo_surrogate_loss,
    #stats_fn=kl_and_loss_stats,
    #extra_action_out_fn=vf_preds_fetches,
    postprocess_fn=compute_gae_for_sample_batch,
    extra_grad_process_fn=apply_grad_clipping,
    before_init=setup_config,
    before_loss_init=setup_mixins,
    mixins=[
        LearningRateSchedule, EntropyCoeffSchedule, KLCoeffMixin,
        ValueNetworkMixin
    ],
)
