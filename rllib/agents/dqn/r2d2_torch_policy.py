"""PyTorch policy class used for R2D2."""

from typing import Dict, Tuple

import gym
import ray
from ray.rllib.agents.dqn.dqn_tf_policy import (PRIO_WEIGHTS,
                                                postprocess_nstep_and_prio)
from ray.rllib.agents.dqn.dqn_torch_policy import \
    build_q_model_and_distribution
from ray.rllib.agents.dqn.r2d2_tf_policy import \
    get_distribution_inputs_and_class
from ray.rllib.agents.dqn.simple_q_torch_policy import TargetNetworkMixin
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import \
    TorchDistributionWrapper
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy import LearningRateSchedule
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_ops import apply_grad_clipping, FLOAT_MIN, \
    huber_loss, reduce_mean_ignore_inf, sequence_mask
from ray.rllib.utils.typing import TensorType, TrainerConfigDict

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional


def build_r2d2_model_and_distribution(
    policy: Policy, obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict) -> \
        Tuple[ModelV2, TorchDistributionWrapper]:
    """Build q_model and target_q_model for DQN

    Args:
        policy (Policy): The policy, which will use the model for optimization.
        obs_space (gym.spaces.Space): The policy's observation space.
        action_space (gym.spaces.Space): The policy's action space.
        config (TrainerConfigDict):

    Returns:
        (q_model, TorchCategorical)
            Note: The target q model will not be returned, just assigned to
            `policy.target_q_model`.
    """

    # Create the policy's models and action dist class.
    model, distribution_cls = build_q_model_and_distribution(
        policy, obs_space, action_space, config)

    # Assert correct model type.
    assert model.get_initial_state() != [], \
        "R2D2 requires its model to be a recurrent one! Try using " \
        "`model.use_lstm` or `model.use_attention` in your config " \
        "to auto-wrap your model with an LSTM- or attention net."

    return model, distribution_cls


def r2d2_loss(policy: Policy, model, _,
              train_batch: SampleBatch) -> TensorType:
    """Constructs the loss for R2D2TorchPolicy.

    Args:
        policy (Policy): The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        train_batch (SampleBatch): The training data.

    Returns:
        TensorType: A single loss tensor.
    """
    config = policy.config
    i = 0
    state_batches = []
    while "state_in_{}".format(i) in train_batch:
        state_batches.append(train_batch["state_in_{}".format(i)])
        i += 1
    assert state_batches

    # Q-network evaluation.
    q, q_logits, q_probs, state_out = compute_q_values(
        policy,
        policy.q_model,
        train_batch,
        state_batches=state_batches,
        seq_lens=train_batch.get("seq_lens"),
        explore=False,
        is_training=True)

    B = state_batches[0].shape[0]
    T = q.shape[0] // B

    # Target Q-network evaluation.
    q_target, q_logits_target, q_probs_target, state_out_target = compute_q_values(
        policy,
        policy.target_q_model,
        train_batch,
        state_batches=state_batches,
        seq_lens=train_batch.get("seq_lens"),
        explore=False,
        is_training=True)

    # Q scores for actions which we know were selected in the given state.
    one_hot_selection = F.one_hot(train_batch[SampleBatch.ACTIONS].long(),
                                  policy.action_space.n)
    q_selected = torch.sum(
        torch.where(q > FLOAT_MIN, q, torch.tensor(0.0, device=policy.device))
        * one_hot_selection, 1)
    #q_logits_selected = torch.sum(
    #    q_logits * torch.unsqueeze(one_hot_selection, -1), 1)

    if config["double_q"]:
        best_actions = torch.argmax(q, 1)
        best_actions_one_hot = F.one_hot(best_actions, policy.action_space.n)
        q_target_best = torch.sum(
            torch.where(q_target > FLOAT_MIN, q_target,
                        torch.tensor(0.0, device=policy.device)) *
            best_actions_one_hot, 1)

    else:
        raise ValueError("non-double Q not supported for R2D2 yet!")
        #q_tp1_best_one_hot_selection = F.one_hot(
        #    torch.argmax(q_tp1, 1), policy.action_space.n)
        #q_tp1_best = torch.sum(
        #    torch.where(q_tp1 > FLOAT_MIN, q_tp1,
        #                torch.tensor(0.0, device=policy.device)) *
        #    q_tp1_best_one_hot_selection, 1)
        #q_probs_tp1_best = torch.sum(
        #    q_probs_tp1 * torch.unsqueeze(q_tp1_best_one_hot_selection, -1), 1)

    if config["num_atoms"] > 1:
        raise ValueError("Distributional R2D2 not supported yet!")
    else:
        q_target_best_masked = (
            1.0 - train_batch[SampleBatch.DONES].float()) * q_target_best

        h_inv = h_inverse(q_target_best_masked, config["epsilon"])

        target = h(train_batch[SampleBatch.REWARDS] + \
                   config["gamma"] ** config["n_step"] * h_inv,
                   config["epsilon"])

        # Seq-mask all loss-related terms.
        seq_mask = sequence_mask(train_batch["seq_lens"], T)[:, :-1]

        # Make sure use the correct time indices:
        # Q(t) - [gamma * r + Q^(t+1)]
        # Compute the error (potentially clipped).
        q_selected = q_selected.reshape([B, T])[:, :-1]
        td_error = q_selected - target.reshape([B, T])[:, 1:].detach()
        td_error = td_error * seq_mask
        weights = train_batch[PRIO_WEIGHTS].reshape([B, T])[:, :-1].float()
        policy._total_loss = torch.mean(weights * huber_loss(td_error))
        policy._td_error = td_error.reshape([-1])
        q_selected = q_selected * seq_mask
        policy._loss_stats = {
            "mean_q": torch.mean(q_selected),
            "min_q": torch.min(q_selected),
            "max_q": torch.max(q_selected),
            "mean_td_error": torch.mean(policy._td_error),
        }

    return policy._total_loss


def h(x, epsilon=1.0):
    """h-function described in the paper [1].

    h(x) = sign(x) * [sqrt(abs(x) + 1) - 1] + epsilon * x
    """
    return torch.sign(x) * (torch.sqrt(torch.abs(x) + 1.0) - 1.0) + epsilon * x


def h_inverse(x, epsilon=1.0):
    """h-function described in the paper [1].

    If x > 0.0:
    h-1(x) = [2eps * x + (2eps + 1) - sqrt(4eps x + (2eps + 1)^2)] /
        (2 * eps^2)

    If x < 0.0:
    h-1(x) = [2eps * x + (2eps + 1) + sqrt(-4eps x + (2eps + 1)^2)] /
        (2 * eps^2)
    """
    two_epsilon = epsilon * 2
    if_x_pos = (two_epsilon * x + (two_epsilon + 1.0) -
                torch.sqrt(4.0 * epsilon * x +
                           (two_epsilon + 1.0)**2)) / (2.0 * epsilon**2)
    if_x_neg = (two_epsilon * x - (two_epsilon + 1.0) +
                torch.sqrt(-4.0 * epsilon * x +
                           (two_epsilon + 1.0)**2)) / (2.0 * epsilon**2)
    return torch.where(x < 0.0, if_x_neg, if_x_pos)


class ComputeTDErrorMixin:
    """Assign the `compute_td_error` method to the R2D2TorchPolicy

    This allows us to prioritize on the worker side.
    """

    def __init__(self):
        def compute_td_error(obs_t, act_t, rew_t, obs_tp1, done_mask,
                             importance_weights):
            input_dict = self._lazy_tensor_dict({SampleBatch.CUR_OBS: obs_t})
            input_dict[SampleBatch.ACTIONS] = act_t
            input_dict[SampleBatch.REWARDS] = rew_t
            input_dict[SampleBatch.NEXT_OBS] = obs_tp1
            input_dict[SampleBatch.DONES] = done_mask
            input_dict[PRIO_WEIGHTS] = importance_weights

            # Do forward pass on loss to update td error attribute
            r2d2_loss(self, self.model, None, input_dict)

            return self._td_error

        self.compute_td_error = compute_td_error


def adam_optimizer(policy: Policy,
                   config: TrainerConfigDict) -> "torch.optim.Optimizer":
    return torch.optim.Adam(
        policy.q_func_vars, lr=policy.cur_lr, eps=config["adam_epsilon"])


def build_q_stats(policy: Policy, batch) -> Dict[str, TensorType]:
    return dict({
        "cur_lr": policy.cur_lr,
    }, **policy._loss_stats)


def setup_early_mixins(policy: Policy, obs_space, action_space,
                       config: TrainerConfigDict) -> None:
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])


def before_loss_init(policy: Policy, obs_space: gym.spaces.Space,
                     action_space: gym.spaces.Space,
                     config: TrainerConfigDict) -> None:
    ComputeTDErrorMixin.__init__(policy)
    TargetNetworkMixin.__init__(policy, obs_space, action_space, config)
    # Move target net to device (this is done automatically for the
    # policy.model, but not for any other models the policy has).
    policy.target_q_model = policy.target_q_model.to(policy.device)


def compute_q_values(policy: Policy,
                     model: ModelV2,
                     input_dict,
                     state_batches,
                     seq_lens,
                     explore,
                     is_training: bool = False):
    config = policy.config

    input_dict["is_training"] = is_training
    model_out, state = model(input_dict, state_batches, seq_lens)

    if config["num_atoms"] > 1:
        (action_scores, z, support_logits_per_action, logits,
         probs_or_logits) = model.get_q_value_distributions(model_out)
    else:
        (action_scores, logits,
         probs_or_logits) = model.get_q_value_distributions(model_out)

    if config["dueling"]:
        state_score = model.get_state_value(model_out)
        if policy.config["num_atoms"] > 1:
            support_logits_per_action_mean = torch.mean(
                support_logits_per_action, dim=1)
            support_logits_per_action_centered = (
                support_logits_per_action - torch.unsqueeze(
                    support_logits_per_action_mean, dim=1))
            support_logits_per_action = torch.unsqueeze(
                state_score, dim=1) + support_logits_per_action_centered
            support_prob_per_action = nn.functional.softmax(
                support_logits_per_action)
            value = torch.sum(z * support_prob_per_action, dim=-1)
            logits = support_logits_per_action
            probs_or_logits = support_prob_per_action
        else:
            advantages_mean = reduce_mean_ignore_inf(action_scores, 1)
            advantages_centered = action_scores - torch.unsqueeze(
                advantages_mean, 1)
            value = state_score + advantages_centered
    else:
        value = action_scores

    return value, logits, probs_or_logits, state


def grad_process_and_td_error_fn(policy: Policy,
                                 optimizer: "torch.optim.Optimizer",
                                 loss: TensorType) -> Dict[str, TensorType]:
    # Clip grads if configured.
    return apply_grad_clipping(policy, optimizer, loss)


def extra_action_out_fn(policy: Policy, input_dict, state_batches, model,
                        action_dist) -> Dict[str, TensorType]:
    return {"q_values": policy.q_values}


def postprocess_fn(policy: Policy,
                               batch: SampleBatch,
                               other_agent=None,
                               episode=None) -> SampleBatch:
    batch = postprocess_nstep_and_prio(policy, batch, other_agent, episode)

    # Burn-in? If yes, assert complete_episodes and add zeros to beginning
    # of the batch (length=burn-in).
    if policy.config.get("burn_in", 0) > 0:
        batch.seq_lens = []
        # max_seq_len=40
        # burn_in=20
        # -20 0 20 40 60 80 100 120
        # |---|-----|-----|-------|
        batch.max_seq_len =

    return batch


R2D2TorchPolicy = build_policy_class(
    name="R2D2TorchPolicy",
    framework="torch",
    loss_fn=r2d2_loss,
    get_default_config=lambda: ray.rllib.agents.dqn.r2d2.DEFAULT_CONFIG,
    make_model_and_action_dist=build_r2d2_model_and_distribution,
    action_distribution_fn=get_distribution_inputs_and_class,
    stats_fn=build_q_stats,
    postprocess_fn=postprocess_fn,
    optimizer_fn=adam_optimizer,
    extra_grad_process_fn=grad_process_and_td_error_fn,
    extra_learn_fetches_fn=lambda policy: {"td_error": policy._td_error},
    extra_action_out_fn=extra_action_out_fn,
    before_init=setup_early_mixins,
    before_loss_init=before_loss_init,
    mixins=[
        TargetNetworkMixin,
        ComputeTDErrorMixin,
        LearningRateSchedule,
    ])
