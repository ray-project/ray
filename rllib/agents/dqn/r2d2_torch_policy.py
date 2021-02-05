"""PyTorch policy class used for R2D2."""

from typing import Dict, List, Optional, Tuple

import gym
import ray
from ray.rllib.agents.dqn.dqn_tf_policy import (
    PRIO_WEIGHTS, Q_SCOPE, Q_TARGET_SCOPE, postprocess_nstep_and_prio)
from ray.rllib.agents.dqn.dqn_torch_model import DQNTorchModel
from ray.rllib.agents.dqn.dqn_torch_policy import build_q_model_and_distribution
from ray.rllib.agents.dqn.simple_q_torch_policy import TargetNetworkMixin
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import (TorchCategorical,
                                                      TorchDistributionWrapper)
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy import LearningRateSchedule
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.exploration.parameter_noise import ParameterNoise
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_ops import apply_grad_clipping, FLOAT_MIN, \
    huber_loss, reduce_mean_ignore_inf, softmax_cross_entropy_with_logits
from ray.rllib.utils.typing import ModelInputDict, TensorType, TrainerConfigDict

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional


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
    # Q-network evaluation.
    q, q_logits, q_probs, state_out = compute_q_values(
        policy,
        policy.q_model,
        train_batch,
        state_batches=state_batches,
        seq_lens=train_batch.get("seq_lens"),
        explore=False,
        is_training=True)

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
        torch.where(q > FLOAT_MIN, q,
                    torch.tensor(0.0, device=policy.device)) *
        one_hot_selection, 1)
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
        ## Distributional Q-learning which corresponds to an entropy loss
        #z = torch.range(
        #    0.0, config["num_atoms"] - 1, dtype=torch.float32).to(train_batch[SampleBatch.REWARDS].device)
        #z = config["v_min"] + z * (config["v_max"] - config["v_min"]) / float(config["num_atoms"] - 1)

        ## (batch_size, 1) * (1, num_atoms) = (batch_size, num_atoms)
        #r_tau = torch.unsqueeze(
        #    train_batch[SampleBatch.REWARDS], -1) + config["gamma"]**config["n_step"] * torch.unsqueeze(
        #        1.0 - train_batch[SampleBatch.DONES].float(), -1) * torch.unsqueeze(z, 0)
        #r_tau = torch.clamp(r_tau, config["v_min"], config["v_max"])
        #b = (r_tau - config["v_min"]) / ((config["v_max"] - config["v_min"]) / float(config["num_atoms"] - 1))
        #lb = torch.floor(b)
        #ub = torch.ceil(b)

        ## Indispensable judgement which is missed in most implementations
        ## when b happens to be an integer, lb == ub, so pr_j(s', a*) will
        ## be discarded because (ub-b) == (b-lb) == 0.
        #floor_equal_ceil = (ub - lb < 0.5).float()

        ## (batch_size, num_atoms, num_atoms)
        #l_project = F.one_hot(lb.long(), config["num_atoms"])
        ## (batch_size, num_atoms, num_atoms)
        #u_project = F.one_hot(ub.long(), config["num_atoms"])
        #ml_delta = q_probs_tp1_best * (ub - b + floor_equal_ceil)
        #mu_delta = q_probs_tp1_best * (b - lb)
        #ml_delta = torch.sum(
        #    l_project * torch.unsqueeze(ml_delta, -1), dim=1)
        #mu_delta = torch.sum(
        #    u_project * torch.unsqueeze(mu_delta, -1), dim=1)
        #m = ml_delta + mu_delta

        ## Rainbow paper claims that using this cross entropy loss for
        ## priority is robust and insensitive to `prioritized_replay_alpha`
        #policy._td_error = softmax_cross_entropy_with_logits(
        #    logits=q_logits_t_selected, labels=m)
        #policy._total_loss = torch.mean(policy._td_error * train_batch[PRIO_WEIGHTS])
        #policy._loss_stats = {
        #    # TODO: better Q stats for dist dqn
        #    "mean_td_error": torch.mean(policy._td_error),
        #}
    else:
        q_target_best_masked = (1.0 - train_batch[SampleBatch.DONES].float()) * q_target_best

        #import numpy as np
        #a = torch.from_numpy(np.arange(-10, 11, dtype=np.float32))
        #print(h_inverse(h(a)))

        target = h(train_batch[SampleBatch.REWARDS] + \
            config["gamma"]**config["n_step"] * h_inverse(q_target_best_masked, config["epsilon"]), config["epsilon"])

        # Compute the error (potentially clipped).
        policy._td_error = q_selected - target.detach()
        policy._total_loss = torch.mean(
            train_batch[PRIO_WEIGHTS].float() * huber_loss(policy._td_error))
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
        h-1(x) = [2eps * x + (2eps + 1) - sqrt(4eps x + (2eps + 1)^2)] / (2 * eps^2)
    If x < 0.0:
        h-1(x) = [2eps * x + (2eps + 1) + sqrt(-4eps x + (2eps + 1)^2)] / (2 * eps^2)
    """
    two_epsilon = epsilon * 2
    if_x_pos = (two_epsilon * x + (two_epsilon + 1.0) - torch.sqrt(4.0 * epsilon * x + (two_epsilon + 1.0)**2)) / (2.0 * epsilon ** 2)
    if_x_neg = (two_epsilon * x - (two_epsilon + 1.0) + torch.sqrt(-4.0 * epsilon * x + (two_epsilon + 1.0)**2)) / (2.0 * epsilon ** 2)
    return torch.where(x < 0.0, if_x_neg, if_x_pos)


class ComputeTDErrorMixin:
    """Assign the `compute_td_error` method to the DQNTorchPolicy

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


def get_distribution_inputs_and_class(
        policy: Policy,
        model: ModelV2,
        *,
        input_dict: ModelInputDict,
        state_batches: Optional[List[TensorType]] = None,
        seq_lens: Optional[TensorType] = None,
        explore: bool = True,
        is_training: bool = False,
        **kwargs) -> Tuple[TensorType, type, List[TensorType]]:
    q_vals, logits, probs_or_logits, state_out = compute_q_values(policy, model, input_dict, state_batches, seq_lens, explore, is_training)
    #q_vals = q_vals[0] if isinstance(q_vals, tuple) else q_vals

    policy.q_values = q_vals
    return policy.q_values, TorchCategorical, state_out  # state-out


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

R2D2TorchPolicy = build_policy_class(
    name="R2D2TorchPolicy",
    framework="torch",
    loss_fn=r2d2_loss,
    get_default_config=lambda: ray.rllib.agents.dqn.r2d2.DEFAULT_CONFIG,
    make_model_and_action_dist=build_q_model_and_distribution,
    action_distribution_fn=get_distribution_inputs_and_class,
    stats_fn=build_q_stats,
    postprocess_fn=postprocess_nstep_and_prio,
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
