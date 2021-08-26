"""
PyTorch policy class used for APPO.

Adapted from VTraceTFPolicy to use the PPO surrogate loss.
Keep in sync with changes to VTraceTFPolicy.
"""

import gym
import numpy as np
import logging
from typing import Type

from ray.rllib.agents.dqn.simple_q_torch_policy import TargetNetworkMixin
import ray.rllib.agents.impala.vtrace_torch as vtrace
from ray.rllib.agents.impala.vtrace_torch_policy import make_time_major, \
    choose_optimizer
from ray.rllib.agents.ppo.appo_tf_policy import make_appo_model, \
    postprocess_trajectory
from ray.rllib.agents.ppo.ppo_torch_policy import ValueNetworkMixin, \
    KLCoeffMixin
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import \
    TorchDistributionWrapper, TorchCategorical
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy import LearningRateSchedule
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_ops import apply_grad_clipping, explained_variance,\
    global_norm, sequence_mask
from ray.rllib.utils.typing import TensorType, TrainerConfigDict

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


def appo_surrogate_loss(policy: Policy, model: ModelV2,
                        dist_class: Type[TorchDistributionWrapper],
                        train_batch: SampleBatch) -> TensorType:
    """Constructs the loss for APPO.

    With IS modifications and V-trace for Advantage Estimation.

    Args:
        policy (Policy): The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        dist_class (Type[ActionDistribution]): The action distr. class.
        train_batch (SampleBatch): The training data.

    Returns:
        Union[TensorType, List[TensorType]]: A single loss tensor or a list
            of loss tensors.
    """
    target_model = policy.target_models[model]

    model_out, _ = model.from_batch(train_batch)
    action_dist = dist_class(model_out, model)

    if isinstance(policy.action_space, gym.spaces.Discrete):
        is_multidiscrete = False
        output_hidden_shape = [policy.action_space.n]
    elif isinstance(policy.action_space,
                    gym.spaces.multi_discrete.MultiDiscrete):
        is_multidiscrete = True
        output_hidden_shape = policy.action_space.nvec.astype(np.int32)
    else:
        is_multidiscrete = False
        output_hidden_shape = 1

    def _make_time_major(*args, **kwargs):
        return make_time_major(policy, train_batch.get(SampleBatch.SEQ_LENS),
                               *args, **kwargs)

    actions = train_batch[SampleBatch.ACTIONS]
    dones = train_batch[SampleBatch.DONES]
    rewards = train_batch[SampleBatch.REWARDS]
    behaviour_logits = train_batch[SampleBatch.ACTION_DIST_INPUTS]

    target_model_out, _ = target_model.from_batch(train_batch)

    prev_action_dist = dist_class(behaviour_logits, model)
    values = model.value_function()
    values_time_major = _make_time_major(values)

    if policy.is_recurrent():
        max_seq_len = torch.max(train_batch[SampleBatch.SEQ_LENS])
        mask = sequence_mask(train_batch[SampleBatch.SEQ_LENS], max_seq_len)
        mask = torch.reshape(mask, [-1])
        mask = _make_time_major(mask, drop_last=policy.config["vtrace"])
        num_valid = torch.sum(mask)

        def reduce_mean_valid(t):
            return torch.sum(t[mask]) / num_valid

    else:
        reduce_mean_valid = torch.mean

    if policy.config["vtrace"]:
        logger.debug("Using V-Trace surrogate loss (vtrace=True)")

        old_policy_behaviour_logits = target_model_out.detach()
        old_policy_action_dist = dist_class(old_policy_behaviour_logits, model)

        if isinstance(output_hidden_shape, (list, tuple, np.ndarray)):
            unpacked_behaviour_logits = torch.split(
                behaviour_logits, list(output_hidden_shape), dim=1)
            unpacked_old_policy_behaviour_logits = torch.split(
                old_policy_behaviour_logits, list(output_hidden_shape), dim=1)
        else:
            unpacked_behaviour_logits = torch.chunk(
                behaviour_logits, output_hidden_shape, dim=1)
            unpacked_old_policy_behaviour_logits = torch.chunk(
                old_policy_behaviour_logits, output_hidden_shape, dim=1)

        # Prepare actions for loss.
        loss_actions = actions if is_multidiscrete else torch.unsqueeze(
            actions, dim=1)

        # Prepare KL for loss.
        action_kl = _make_time_major(
            old_policy_action_dist.kl(action_dist), drop_last=True)

        # Compute vtrace on the CPU for better perf.
        vtrace_returns = vtrace.multi_from_logits(
            behaviour_policy_logits=_make_time_major(
                unpacked_behaviour_logits, drop_last=True),
            target_policy_logits=_make_time_major(
                unpacked_old_policy_behaviour_logits, drop_last=True),
            actions=torch.unbind(
                _make_time_major(loss_actions, drop_last=True), dim=2),
            discounts=(1.0 - _make_time_major(dones, drop_last=True).float()) *
            policy.config["gamma"],
            rewards=_make_time_major(rewards, drop_last=True),
            values=values_time_major[:-1],  # drop-last=True
            bootstrap_value=values_time_major[-1],
            dist_class=TorchCategorical if is_multidiscrete else dist_class,
            model=model,
            clip_rho_threshold=policy.config["vtrace_clip_rho_threshold"],
            clip_pg_rho_threshold=policy.config[
                "vtrace_clip_pg_rho_threshold"])

        actions_logp = _make_time_major(
            action_dist.logp(actions), drop_last=True)
        prev_actions_logp = _make_time_major(
            prev_action_dist.logp(actions), drop_last=True)
        old_policy_actions_logp = _make_time_major(
            old_policy_action_dist.logp(actions), drop_last=True)
        is_ratio = torch.clamp(
            torch.exp(prev_actions_logp - old_policy_actions_logp), 0.0, 2.0)
        logp_ratio = is_ratio * torch.exp(actions_logp - prev_actions_logp)
        policy._is_ratio = is_ratio

        advantages = vtrace_returns.pg_advantages.to(logp_ratio.device)
        surrogate_loss = torch.min(
            advantages * logp_ratio,
            advantages *
            torch.clamp(logp_ratio, 1 - policy.config["clip_param"],
                        1 + policy.config["clip_param"]))

        mean_kl = reduce_mean_valid(action_kl)
        mean_policy_loss = -reduce_mean_valid(surrogate_loss)

        # The value function loss.
        value_targets = vtrace_returns.vs.to(values_time_major.device)
        delta = values_time_major[:-1] - value_targets
        mean_vf_loss = 0.5 * reduce_mean_valid(torch.pow(delta, 2.0))

        # The entropy loss.
        mean_entropy = reduce_mean_valid(
            _make_time_major(action_dist.entropy(), drop_last=True))

    else:
        logger.debug("Using PPO surrogate loss (vtrace=False)")

        # Prepare KL for Loss
        action_kl = _make_time_major(prev_action_dist.kl(action_dist))

        actions_logp = _make_time_major(action_dist.logp(actions))
        prev_actions_logp = _make_time_major(prev_action_dist.logp(actions))
        logp_ratio = torch.exp(actions_logp - prev_actions_logp)

        advantages = _make_time_major(train_batch[Postprocessing.ADVANTAGES])
        surrogate_loss = torch.min(
            advantages * logp_ratio,
            advantages *
            torch.clamp(logp_ratio, 1 - policy.config["clip_param"],
                        1 + policy.config["clip_param"]))

        mean_kl = reduce_mean_valid(action_kl)
        mean_policy_loss = -reduce_mean_valid(surrogate_loss)

        # The value function loss.
        value_targets = _make_time_major(
            train_batch[Postprocessing.VALUE_TARGETS])
        delta = values_time_major - value_targets
        mean_vf_loss = 0.5 * reduce_mean_valid(torch.pow(delta, 2.0))

        # The entropy loss.
        mean_entropy = reduce_mean_valid(
            _make_time_major(action_dist.entropy()))

    # The summed weighted loss
    total_loss = mean_policy_loss + \
        mean_vf_loss * policy.config["vf_loss_coeff"] - \
        mean_entropy * policy.config["entropy_coeff"]

    # Optional additional KL Loss
    if policy.config["use_kl_loss"]:
        total_loss += policy.kl_coeff * mean_kl

    policy._total_loss = total_loss
    policy._mean_policy_loss = mean_policy_loss
    policy._mean_kl = mean_kl
    policy._mean_vf_loss = mean_vf_loss
    policy._mean_entropy = mean_entropy
    policy._value_targets = value_targets
    policy._vf_explained_var = explained_variance(
        torch.reshape(value_targets, [-1]),
        torch.reshape(
            values_time_major[:-1]
            if policy.config["vtrace"] else values_time_major, [-1]),
    )

    return total_loss


def stats(policy: Policy, train_batch: SampleBatch):
    """Stats function for APPO. Returns a dict with important loss stats.

    Args:
        policy (Policy): The Policy to generate stats for.
        train_batch (SampleBatch): The SampleBatch (already) used for training.

    Returns:
        Dict[str, TensorType]: The stats dict.
    """
    stats_dict = {
        "cur_lr": policy.cur_lr,
        "policy_loss": policy._mean_policy_loss,
        "entropy": policy._mean_entropy,
        "var_gnorm": global_norm(policy.model.trainable_variables()),
        "vf_loss": policy._mean_vf_loss,
        "vf_explained_var": policy._vf_explained_var,
    }

    if policy.config["vtrace"]:
        is_stat_mean = torch.mean(policy._is_ratio, [0, 1])
        is_stat_var = torch.var(policy._is_ratio, [0, 1])
        stats_dict.update({"mean_IS": is_stat_mean})
        stats_dict.update({"var_IS": is_stat_var})

    if policy.config["use_kl_loss"]:
        stats_dict.update({"kl": policy._mean_kl})
        stats_dict.update({"KL_Coeff": policy.kl_coeff})

    return stats_dict


def add_values(policy, input_dict, state_batches, model, action_dist):
    out = {}
    if not policy.config["vtrace"]:
        out[SampleBatch.VF_PREDS] = model.value_function()
    return out


def setup_early_mixins(policy: Policy, obs_space: gym.spaces.Space,
                       action_space: gym.spaces.Space,
                       config: TrainerConfigDict):
    """Call all mixin classes' constructors before APPOPolicy initialization.

    Args:
        policy (Policy): The Policy object.
        obs_space (gym.spaces.Space): The Policy's observation space.
        action_space (gym.spaces.Space): The Policy's action space.
        config (TrainerConfigDict): The Policy's config.
    """
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])


def setup_late_mixins(policy: Policy, obs_space: gym.spaces.Space,
                      action_space: gym.spaces.Space,
                      config: TrainerConfigDict):
    """Call all mixin classes' constructors after APPOPolicy initialization.

    Args:
        policy (Policy): The Policy object.
        obs_space (gym.spaces.Space): The Policy's observation space.
        action_space (gym.spaces.Space): The Policy's action space.
        config (TrainerConfigDict): The Policy's config.
    """
    KLCoeffMixin.__init__(policy, config)
    ValueNetworkMixin.__init__(policy, obs_space, action_space, config)
    TargetNetworkMixin.__init__(policy)


# Build a child class of `TorchPolicy`, given the custom functions defined
# above.
AsyncPPOTorchPolicy = build_policy_class(
    name="AsyncPPOTorchPolicy",
    framework="torch",
    loss_fn=appo_surrogate_loss,
    stats_fn=stats,
    postprocess_fn=postprocess_trajectory,
    extra_action_out_fn=add_values,
    extra_grad_process_fn=apply_grad_clipping,
    optimizer_fn=choose_optimizer,
    before_init=setup_early_mixins,
    before_loss_init=setup_late_mixins,
    make_model=make_appo_model,
    mixins=[
        LearningRateSchedule, KLCoeffMixin, TargetNetworkMixin,
        ValueNetworkMixin
    ],
    get_batch_divisibility_req=lambda p: p.config["rollout_fragment_length"])
