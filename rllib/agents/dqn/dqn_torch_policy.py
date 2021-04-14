"""PyTorch policy class used for DQN"""

from typing import Dict, List, Tuple

import gym
import ray
from ray.rllib.agents.dqn.dqn_tf_policy import (
    PRIO_WEIGHTS, Q_SCOPE, Q_TARGET_SCOPE, postprocess_nstep_and_prio)
from ray.rllib.agents.dqn.dqn_torch_model import DQNTorchModel
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
from ray.rllib.utils.typing import TensorType, TrainerConfigDict

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional


class QLoss:
    def __init__(self,
                 q_t_selected: TensorType,
                 q_logits_t_selected: TensorType,
                 q_tp1_best: TensorType,
                 q_probs_tp1_best: TensorType,
                 importance_weights: TensorType,
                 rewards: TensorType,
                 done_mask: TensorType,
                 gamma=0.99,
                 n_step=1,
                 num_atoms=1,
                 v_min=-10.0,
                 v_max=10.0):

        if num_atoms > 1:
            # Distributional Q-learning which corresponds to an entropy loss
            z = torch.range(
                0.0, num_atoms - 1, dtype=torch.float32).to(rewards.device)
            z = v_min + z * (v_max - v_min) / float(num_atoms - 1)

            # (batch_size, 1) * (1, num_atoms) = (batch_size, num_atoms)
            r_tau = torch.unsqueeze(
                rewards, -1) + gamma**n_step * torch.unsqueeze(
                    1.0 - done_mask, -1) * torch.unsqueeze(z, 0)
            r_tau = torch.clamp(r_tau, v_min, v_max)
            b = (r_tau - v_min) / ((v_max - v_min) / float(num_atoms - 1))
            lb = torch.floor(b)
            ub = torch.ceil(b)

            # Indispensable judgement which is missed in most implementations
            # when b happens to be an integer, lb == ub, so pr_j(s', a*) will
            # be discarded because (ub-b) == (b-lb) == 0.
            floor_equal_ceil = (ub - lb < 0.5).float()

            # (batch_size, num_atoms, num_atoms)
            l_project = F.one_hot(lb.long(), num_atoms)
            # (batch_size, num_atoms, num_atoms)
            u_project = F.one_hot(ub.long(), num_atoms)
            ml_delta = q_probs_tp1_best * (ub - b + floor_equal_ceil)
            mu_delta = q_probs_tp1_best * (b - lb)
            ml_delta = torch.sum(
                l_project * torch.unsqueeze(ml_delta, -1), dim=1)
            mu_delta = torch.sum(
                u_project * torch.unsqueeze(mu_delta, -1), dim=1)
            m = ml_delta + mu_delta

            # Rainbow paper claims that using this cross entropy loss for
            # priority is robust and insensitive to `prioritized_replay_alpha`
            self.td_error = softmax_cross_entropy_with_logits(
                logits=q_logits_t_selected, labels=m)
            self.loss = torch.mean(self.td_error * importance_weights)
            self.stats = {
                # TODO: better Q stats for dist dqn
                "mean_td_error": torch.mean(self.td_error),
            }
        else:
            q_tp1_best_masked = (1.0 - done_mask) * q_tp1_best

            # compute RHS of bellman equation
            q_t_selected_target = rewards + gamma**n_step * q_tp1_best_masked

            # compute the error (potentially clipped)
            self.td_error = q_t_selected - q_t_selected_target.detach()
            self.loss = torch.mean(
                importance_weights.float() * huber_loss(self.td_error))
            self.stats = {
                "mean_q": torch.mean(q_t_selected),
                "min_q": torch.min(q_t_selected),
                "max_q": torch.max(q_t_selected),
                "mean_td_error": torch.mean(self.td_error),
            }


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
            build_q_losses(self, self.model, None, input_dict)

            return self.q_loss.td_error

        self.compute_td_error = compute_td_error


def build_q_model_and_distribution(
        policy: Policy, obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: TrainerConfigDict) -> Tuple[ModelV2, TorchDistributionWrapper]:
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
    if not isinstance(action_space, gym.spaces.Discrete):
        raise UnsupportedSpaceException(
            "Action space {} is not supported for DQN.".format(action_space))

    if config["hiddens"]:
        # try to infer the last layer size, otherwise fall back to 256
        num_outputs = ([256] + list(config["model"]["fcnet_hiddens"]))[-1]
        config["model"]["no_final_linear"] = True
    else:
        num_outputs = action_space.n

    # TODO(sven): Move option to add LayerNorm after each Dense
    #  generically into ModelCatalog.
    add_layer_norm = (
        isinstance(getattr(policy, "exploration", None), ParameterNoise)
        or config["exploration_config"]["type"] == "ParameterNoise")

    model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=num_outputs,
        model_config=config["model"],
        framework="torch",
        model_interface=DQNTorchModel,
        name=Q_SCOPE,
        q_hiddens=config["hiddens"],
        dueling=config["dueling"],
        num_atoms=config["num_atoms"],
        use_noisy=config["noisy"],
        v_min=config["v_min"],
        v_max=config["v_max"],
        sigma0=config["sigma0"],
        # TODO(sven): Move option to add LayerNorm after each Dense
        #  generically into ModelCatalog.
        add_layer_norm=add_layer_norm)

    policy.q_func_vars = model.variables()

    policy.target_q_model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=num_outputs,
        model_config=config["model"],
        framework="torch",
        model_interface=DQNTorchModel,
        name=Q_TARGET_SCOPE,
        q_hiddens=config["hiddens"],
        dueling=config["dueling"],
        num_atoms=config["num_atoms"],
        use_noisy=config["noisy"],
        v_min=config["v_min"],
        v_max=config["v_max"],
        sigma0=config["sigma0"],
        # TODO(sven): Move option to add LayerNorm after each Dense
        #  generically into ModelCatalog.
        add_layer_norm=add_layer_norm)

    policy.target_q_func_vars = policy.target_q_model.variables()

    return model, TorchCategorical


def get_distribution_inputs_and_class(
        policy: Policy,
        model: ModelV2,
        obs_batch: TensorType,
        *,
        explore: bool = True,
        is_training: bool = False,
        **kwargs) -> Tuple[TensorType, type, List[TensorType]]:
    q_vals = compute_q_values(
        policy,
        model, {"obs": obs_batch},
        explore=explore,
        is_training=is_training)
    q_vals = q_vals[0] if isinstance(q_vals, tuple) else q_vals

    policy.q_values = q_vals
    return policy.q_values, TorchCategorical, []  # state-out


def build_q_losses(policy: Policy, model, _,
                   train_batch: SampleBatch) -> TensorType:
    """Constructs the loss for DQNTorchPolicy.

    Args:
        policy (Policy): The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        train_batch (SampleBatch): The training data.

    Returns:
        TensorType: A single loss tensor.
    """
    config = policy.config
    # Q-network evaluation.
    q_t, q_logits_t, q_probs_t, _ = compute_q_values(
        policy,
        model, {"obs": train_batch[SampleBatch.CUR_OBS]},
        explore=False,
        is_training=True)

    # Target Q-network evaluation.
    q_tp1, q_logits_tp1, q_probs_tp1, _ = compute_q_values(
        policy,
        policy.target_q_model, {"obs": train_batch[SampleBatch.NEXT_OBS]},
        explore=False,
        is_training=True)

    # Q scores for actions which we know were selected in the given state.
    one_hot_selection = F.one_hot(train_batch[SampleBatch.ACTIONS].long(),
                                  policy.action_space.n)
    q_t_selected = torch.sum(
        torch.where(q_t > FLOAT_MIN, q_t,
                    torch.tensor(0.0, device=policy.device)) *
        one_hot_selection, 1)
    q_logits_t_selected = torch.sum(
        q_logits_t * torch.unsqueeze(one_hot_selection, -1), 1)

    # compute estimate of best possible value starting from state at t + 1
    if config["double_q"]:
        q_tp1_using_online_net, q_logits_tp1_using_online_net, \
            q_dist_tp1_using_online_net, _ = compute_q_values(
                policy,
                model,
                {"obs": train_batch[SampleBatch.NEXT_OBS]},
                explore=False,
                is_training=True)
        q_tp1_best_using_online_net = torch.argmax(q_tp1_using_online_net, 1)
        q_tp1_best_one_hot_selection = F.one_hot(q_tp1_best_using_online_net,
                                                 policy.action_space.n)
        q_tp1_best = torch.sum(
            torch.where(q_tp1 > FLOAT_MIN, q_tp1,
                        torch.tensor(0.0, device=policy.device)) *
            q_tp1_best_one_hot_selection, 1)
        q_probs_tp1_best = torch.sum(
            q_probs_tp1 * torch.unsqueeze(q_tp1_best_one_hot_selection, -1), 1)
    else:
        q_tp1_best_one_hot_selection = F.one_hot(
            torch.argmax(q_tp1, 1), policy.action_space.n)
        q_tp1_best = torch.sum(
            torch.where(q_tp1 > FLOAT_MIN, q_tp1,
                        torch.tensor(0.0, device=policy.device)) *
            q_tp1_best_one_hot_selection, 1)
        q_probs_tp1_best = torch.sum(
            q_probs_tp1 * torch.unsqueeze(q_tp1_best_one_hot_selection, -1), 1)

    policy.q_loss = QLoss(
        q_t_selected, q_logits_t_selected, q_tp1_best, q_probs_tp1_best,
        train_batch[PRIO_WEIGHTS], train_batch[SampleBatch.REWARDS],
        train_batch[SampleBatch.DONES].float(), config["gamma"],
        config["n_step"], config["num_atoms"], config["v_min"],
        config["v_max"])

    return policy.q_loss.loss


def adam_optimizer(policy: Policy,
                   config: TrainerConfigDict) -> "torch.optim.Optimizer":
    return torch.optim.Adam(
        policy.q_func_vars, lr=policy.cur_lr, eps=config["adam_epsilon"])


def build_q_stats(policy: Policy, batch) -> Dict[str, TensorType]:
    return dict({
        "cur_lr": policy.cur_lr,
    }, **policy.q_loss.stats)


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
                     state_batches=None,
                     seq_lens=None,
                     explore=None,
                     is_training: bool = False):
    config = policy.config

    input_dict["is_training"] = is_training
    model_out, state = model(input_dict, state_batches or [], seq_lens)

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


DQNTorchPolicy = build_policy_class(
    name="DQNTorchPolicy",
    framework="torch",
    loss_fn=build_q_losses,
    get_default_config=lambda: ray.rllib.agents.dqn.dqn.DEFAULT_CONFIG,
    make_model_and_action_dist=build_q_model_and_distribution,
    action_distribution_fn=get_distribution_inputs_and_class,
    stats_fn=build_q_stats,
    postprocess_fn=postprocess_nstep_and_prio,
    optimizer_fn=adam_optimizer,
    extra_grad_process_fn=grad_process_and_td_error_fn,
    extra_learn_fetches_fn=lambda policy: {"td_error": policy.q_loss.td_error},
    extra_action_out_fn=extra_action_out_fn,
    before_init=setup_early_mixins,
    before_loss_init=before_loss_init,
    mixins=[
        TargetNetworkMixin,
        ComputeTDErrorMixin,
        LearningRateSchedule,
    ])
