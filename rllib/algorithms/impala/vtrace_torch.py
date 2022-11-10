# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""PyTorch version of the functions to compute V-trace off-policy actor critic
targets.

For details and theory see:

"IMPALA: Scalable Distributed Deep-RL with
Importance Weighted Actor-Learner Architectures"
by Espeholt, Soyer, Munos et al.

See https://arxiv.org/abs/1802.01561 for the full paper.

In addition to the original paper's code, changes have been made
to support MultiDiscrete action spaces. behaviour_policy_logits,
target_policy_logits and actions parameters in the entry point
multi_from_logits method accepts lists of tensors instead of just
tensors.
"""

from ray.rllib.algorithms.impala.vtrace_tf import VTraceFromLogitsReturns, VTraceReturns
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.utils import force_list
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor

torch, nn = try_import_torch()


def log_probs_from_logits_and_actions(
    policy_logits, actions, dist_class=TorchCategorical, model=None
):
    return multi_log_probs_from_logits_and_actions(
        [policy_logits], [actions], dist_class, model
    )[0]


def multi_log_probs_from_logits_and_actions(policy_logits, actions, dist_class, model):
    """Computes action log-probs from policy logits and actions.

    In the notation used throughout documentation and comments, T refers to the
    time dimension ranging from 0 to T-1. B refers to the batch size and
    ACTION_SPACE refers to the list of numbers each representing a number of
    actions.

    Args:
        policy_logits: A list with length of ACTION_SPACE of float32
            tensors of shapes [T, B, ACTION_SPACE[0]], ...,
            [T, B, ACTION_SPACE[-1]] with un-normalized log-probabilities
            parameterizing a softmax policy.
        actions: A list with length of ACTION_SPACE of tensors of shapes
            [T, B, ...], ..., [T, B, ...]
            with actions.
        dist_class: Python class of the action distribution.

    Returns:
        A list with length of ACTION_SPACE of float32 tensors of shapes
            [T, B], ..., [T, B] corresponding to the sampling log probability
            of the chosen action w.r.t. the policy.
    """
    log_probs = []
    for i in range(len(policy_logits)):
        p_shape = policy_logits[i].shape
        a_shape = actions[i].shape
        policy_logits_flat = torch.reshape(policy_logits[i], (-1,) + tuple(p_shape[2:]))
        actions_flat = torch.reshape(actions[i], (-1,) + tuple(a_shape[2:]))
        log_probs.append(
            torch.reshape(
                dist_class(policy_logits_flat, model).logp(actions_flat), a_shape[:2]
            )
        )

    return log_probs


def from_logits(
    behaviour_policy_logits,
    target_policy_logits,
    actions,
    discounts,
    rewards,
    values,
    bootstrap_value,
    dist_class=TorchCategorical,
    model=None,
    clip_rho_threshold=1.0,
    clip_pg_rho_threshold=1.0,
):
    """multi_from_logits wrapper used only for tests"""

    res = multi_from_logits(
        [behaviour_policy_logits],
        [target_policy_logits],
        [actions],
        discounts,
        rewards,
        values,
        bootstrap_value,
        dist_class,
        model,
        clip_rho_threshold=clip_rho_threshold,
        clip_pg_rho_threshold=clip_pg_rho_threshold,
    )

    assert len(res.behaviour_action_log_probs) == 1
    assert len(res.target_action_log_probs) == 1
    return VTraceFromLogitsReturns(
        vs=res.vs,
        pg_advantages=res.pg_advantages,
        log_rhos=res.log_rhos,
        behaviour_action_log_probs=res.behaviour_action_log_probs[0],
        target_action_log_probs=res.target_action_log_probs[0],
    )


def multi_from_logits(
    behaviour_policy_logits,
    target_policy_logits,
    actions,
    discounts,
    rewards,
    values,
    bootstrap_value,
    dist_class,
    model,
    behaviour_action_log_probs=None,
    clip_rho_threshold=1.0,
    clip_pg_rho_threshold=1.0,
):
    r"""V-trace for softmax policies.

    Calculates V-trace actor critic targets for softmax polices as described in

    "IMPALA: Scalable Distributed Deep-RL with
    Importance Weighted Actor-Learner Architectures"
    by Espeholt, Soyer, Munos et al.

    Target policy refers to the policy we are interested in improving and
    behaviour policy refers to the policy that generated the given
    rewards and actions.

    In the notation used throughout documentation and comments, T refers to the
    time dimension ranging from 0 to T-1. B refers to the batch size and
    ACTION_SPACE refers to the list of numbers each representing a number of
    actions.

    Args:
        behaviour_policy_logits: A list with length of ACTION_SPACE of float32
            tensors of shapes [T, B, ACTION_SPACE[0]], ...,
            [T, B, ACTION_SPACE[-1]] with un-normalized log-probabilities
            parameterizing the softmax behavior policy.
        target_policy_logits: A list with length of ACTION_SPACE of float32
            tensors of shapes [T, B, ACTION_SPACE[0]], ...,
            [T, B, ACTION_SPACE[-1]] with un-normalized log-probabilities
            parameterizing the softmax target policy.
        actions: A list with length of ACTION_SPACE of tensors of shapes
            [T, B, ...], ..., [T, B, ...]
            with actions sampled from the behavior policy.
        discounts: A float32 tensor of shape [T, B] with the discount
            encountered when following the behavior policy.
        rewards: A float32 tensor of shape [T, B] with the rewards generated by
            following the behavior policy.
        values: A float32 tensor of shape [T, B] with the value function
            estimates wrt. the target policy.
        bootstrap_value: A float32 of shape [B] with the value function
            estimate at time T.
        dist_class: action distribution class for the logits.
        model: backing ModelV2 instance
        behaviour_action_log_probs: Precalculated values of the behavior
            actions.
        clip_rho_threshold: A scalar float32 tensor with the clipping threshold
            for importance weights (rho) when calculating the baseline targets
            (vs). rho^bar in the paper.
        clip_pg_rho_threshold: A scalar float32 tensor with the clipping
            threshold on rho_s in:
            \rho_s \delta log \pi(a|x) (r + \gamma v_{s+1} - V(x_s)).

    Returns:
        A `VTraceFromLogitsReturns` namedtuple with the following fields:
        vs: A float32 tensor of shape [T, B]. Can be used as target to train a
            baseline (V(x_t) - vs_t)^2.
        pg_advantages: A float 32 tensor of shape [T, B]. Can be used as an
            estimate of the advantage in the calculation of policy gradients.
        log_rhos: A float32 tensor of shape [T, B] containing the log
            importance sampling weights (log rhos).
        behaviour_action_log_probs: A float32 tensor of shape [T, B] containing
            behaviour policy action log probabilities (log \mu(a_t)).
        target_action_log_probs: A float32 tensor of shape [T, B] containing
            target policy action probabilities (log \pi(a_t)).
    """

    behaviour_policy_logits = convert_to_torch_tensor(
        behaviour_policy_logits, device="cpu"
    )
    target_policy_logits = convert_to_torch_tensor(target_policy_logits, device="cpu")
    actions = convert_to_torch_tensor(actions, device="cpu")

    # Make sure tensor ranks are as expected.
    # The rest will be checked by from_action_log_probs.
    for i in range(len(behaviour_policy_logits)):
        assert len(behaviour_policy_logits[i].size()) == 3
        assert len(target_policy_logits[i].size()) == 3

    target_action_log_probs = multi_log_probs_from_logits_and_actions(
        target_policy_logits, actions, dist_class, model
    )

    if len(behaviour_policy_logits) > 1 or behaviour_action_log_probs is None:
        # can't use precalculated values, recompute them. Note that
        # recomputing won't work well for autoregressive action dists
        # which may have variables not captured by 'logits'
        behaviour_action_log_probs = multi_log_probs_from_logits_and_actions(
            behaviour_policy_logits, actions, dist_class, model
        )

    behaviour_action_log_probs = convert_to_torch_tensor(
        behaviour_action_log_probs, device="cpu"
    )
    behaviour_action_log_probs = force_list(behaviour_action_log_probs)
    log_rhos = get_log_rhos(target_action_log_probs, behaviour_action_log_probs)

    vtrace_returns = from_importance_weights(
        log_rhos=log_rhos,
        discounts=discounts,
        rewards=rewards,
        values=values,
        bootstrap_value=bootstrap_value,
        clip_rho_threshold=clip_rho_threshold,
        clip_pg_rho_threshold=clip_pg_rho_threshold,
    )

    return VTraceFromLogitsReturns(
        log_rhos=log_rhos,
        behaviour_action_log_probs=behaviour_action_log_probs,
        target_action_log_probs=target_action_log_probs,
        **vtrace_returns._asdict()
    )


def from_importance_weights(
    log_rhos,
    discounts,
    rewards,
    values,
    bootstrap_value,
    clip_rho_threshold=1.0,
    clip_pg_rho_threshold=1.0,
):
    r"""V-trace from log importance weights.

    Calculates V-trace actor critic targets as described in

    "IMPALA: Scalable Distributed Deep-RL with
    Importance Weighted Actor-Learner Architectures"
    by Espeholt, Soyer, Munos et al.

    In the notation used throughout documentation and comments, T refers to the
    time dimension ranging from 0 to T-1. B refers to the batch size. This code
    also supports the case where all tensors have the same number of additional
    dimensions, e.g., `rewards` is [T, B, C], `values` is [T, B, C],
    `bootstrap_value` is [B, C].

    Args:
        log_rhos: A float32 tensor of shape [T, B] representing the log
            importance sampling weights, i.e.
            log(target_policy(a) / behaviour_policy(a)). V-trace performs
            operations on rhos in log-space for numerical stability.
        discounts: A float32 tensor of shape [T, B] with discounts encountered
            when following the behaviour policy.
        rewards: A float32 tensor of shape [T, B] containing rewards generated
            by following the behaviour policy.
        values: A float32 tensor of shape [T, B] with the value function
            estimates wrt. the target policy.
        bootstrap_value: A float32 of shape [B] with the value function
            estimate at time T.
        clip_rho_threshold: A scalar float32 tensor with the clipping threshold
            for importance weights (rho) when calculating the baseline targets
            (vs). rho^bar in the paper. If None, no clipping is applied.
        clip_pg_rho_threshold: A scalar float32 tensor with the clipping
            threshold on rho_s in
            \rho_s \delta log \pi(a|x) (r + \gamma v_{s+1} - V(x_s)).
            If None, no clipping is applied.

    Returns:
        A VTraceReturns namedtuple (vs, pg_advantages) where:
        vs: A float32 tensor of shape [T, B]. Can be used as target to
            train a baseline (V(x_t) - vs_t)^2.
        pg_advantages: A float32 tensor of shape [T, B]. Can be used as the
            advantage in the calculation of policy gradients.
    """
    log_rhos = convert_to_torch_tensor(log_rhos, device="cpu")
    discounts = convert_to_torch_tensor(discounts, device="cpu")
    rewards = convert_to_torch_tensor(rewards, device="cpu")
    values = convert_to_torch_tensor(values, device="cpu")
    bootstrap_value = convert_to_torch_tensor(bootstrap_value, device="cpu")

    # Make sure tensor ranks are consistent.
    rho_rank = len(log_rhos.size())  # Usually 2.
    assert rho_rank == len(values.size())
    assert rho_rank - 1 == len(bootstrap_value.size()), "must have rank {}".format(
        rho_rank - 1
    )
    assert rho_rank == len(discounts.size())
    assert rho_rank == len(rewards.size())

    rhos = torch.exp(log_rhos)
    if clip_rho_threshold is not None:
        clipped_rhos = torch.clamp_max(rhos, clip_rho_threshold)
    else:
        clipped_rhos = rhos

    cs = torch.clamp_max(rhos, 1.0)
    # Append bootstrapped value to get [v1, ..., v_t+1]
    values_t_plus_1 = torch.cat(
        [values[1:], torch.unsqueeze(bootstrap_value, 0)], dim=0
    )
    deltas = clipped_rhos * (rewards + discounts * values_t_plus_1 - values)

    vs_minus_v_xs = [torch.zeros_like(bootstrap_value)]
    for i in reversed(range(len(discounts))):
        discount_t, c_t, delta_t = discounts[i], cs[i], deltas[i]
        vs_minus_v_xs.append(delta_t + discount_t * c_t * vs_minus_v_xs[-1])
    vs_minus_v_xs = torch.stack(vs_minus_v_xs[1:])
    # Reverse the results back to original order.
    vs_minus_v_xs = torch.flip(vs_minus_v_xs, dims=[0])
    # Add V(x_s) to get v_s.
    vs = vs_minus_v_xs + values

    # Advantage for policy gradient.
    vs_t_plus_1 = torch.cat([vs[1:], torch.unsqueeze(bootstrap_value, 0)], dim=0)
    if clip_pg_rho_threshold is not None:
        clipped_pg_rhos = torch.clamp_max(rhos, clip_pg_rho_threshold)
    else:
        clipped_pg_rhos = rhos
    pg_advantages = clipped_pg_rhos * (rewards + discounts * vs_t_plus_1 - values)

    # Make sure no gradients backpropagated through the returned values.
    return VTraceReturns(vs=vs.detach(), pg_advantages=pg_advantages.detach())


def get_log_rhos(target_action_log_probs, behaviour_action_log_probs):
    """With the selected log_probs for multi-discrete actions of behavior
    and target policies we compute the log_rhos for calculating the vtrace."""
    t = torch.stack(target_action_log_probs)
    b = torch.stack(behaviour_action_log_probs)
    log_rhos = torch.sum(t - b, dim=0)
    return log_rhos
