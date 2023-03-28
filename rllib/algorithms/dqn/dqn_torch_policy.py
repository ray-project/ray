"""PyTorch policy class used for DQN"""

from typing import Dict, List, Tuple

import gymnasium as gym
import ray
from ray.rllib.algorithms.dqn.dqn_tf_policy import (
    PRIO_WEIGHTS,
    Q_SCOPE,
    Q_TARGET_SCOPE,
    postprocess_nstep_and_prio,
)
from ray.rllib.algorithms.dqn.dqn_torch_model import DQNTorchModel
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import (
    get_torch_categorical_class_with_temperature,
    TorchDistributionWrapper,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_mixins import (
    LearningRateSchedule,
    TargetNetworkMixin,
)

from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.exploration.parameter_noise import ParameterNoise
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import (
    apply_grad_clipping,
    concat_multi_gpu_td_errors,
    FLOAT_MIN,
    huber_loss,
    l2_loss,
    reduce_mean_ignore_inf,
    softmax_cross_entropy_with_logits,
)
from ray.rllib.utils.typing import TensorType, AlgorithmConfigDict

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional


class QLoss:
    def __init__(
        self,
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
        v_max=10.0,
        loss_fn=huber_loss,
    ):

        if num_atoms > 1:
            # Distributional Q-learning which corresponds to an entropy loss
            z = torch.arange(0.0, num_atoms, dtype=torch.float32).to(rewards.device)
            z = v_min + z * (v_max - v_min) / float(num_atoms - 1)

            # (batch_size, 1) * (1, num_atoms) = (batch_size, num_atoms)
            r_tau = torch.unsqueeze(rewards, -1) + gamma**n_step * torch.unsqueeze(
                1.0 - done_mask, -1
            ) * torch.unsqueeze(z, 0)
            r_tau = torch.clamp(r_tau, v_min, v_max)
            b = (r_tau - v_min) / ((v_max - v_min) / float(num_atoms - 1))
            lb = torch.floor(b)
            ub = torch.ceil(b)

            # Indispensable judgement which is missed in most implementations
            # when b happens to be an integer, lb == ub, so pr_j(s', a*) will
            # be discarded because (ub-b) == (b-lb) == 0.
            floor_equal_ceil = ((ub - lb) < 0.5).float()

            # (batch_size, num_atoms, num_atoms)
            l_project = F.one_hot(lb.long(), num_atoms)
            # (batch_size, num_atoms, num_atoms)
            u_project = F.one_hot(ub.long(), num_atoms)
            ml_delta = q_probs_tp1_best * (ub - b + floor_equal_ceil)
            mu_delta = q_probs_tp1_best * (b - lb)
            ml_delta = torch.sum(l_project * torch.unsqueeze(ml_delta, -1), dim=1)
            mu_delta = torch.sum(u_project * torch.unsqueeze(mu_delta, -1), dim=1)
            m = ml_delta + mu_delta

            # Rainbow paper claims that using this cross entropy loss for
            # priority is robust and insensitive to `prioritized_replay_alpha`
            self.td_error = softmax_cross_entropy_with_logits(
                logits=q_logits_t_selected, labels=m.detach()
            )
            self.loss = torch.mean(self.td_error * importance_weights)
            self.stats = {
                # TODO: better Q stats for dist dqn
            }
        else:
            q_tp1_best_masked = (1.0 - done_mask) * q_tp1_best

            # compute RHS of bellman equation
            q_t_selected_target = rewards + gamma**n_step * q_tp1_best_masked

            # compute the error (potentially clipped)
            self.td_error = q_t_selected - q_t_selected_target.detach()
            self.loss = torch.mean(importance_weights.float() * loss_fn(self.td_error))
            self.stats = {
                "mean_q": torch.mean(q_t_selected),
                "min_q": torch.min(q_t_selected),
                "max_q": torch.max(q_t_selected),
            }


class ComputeTDErrorMixin:
    """Assign the `compute_td_error` method to the DQNTorchPolicy

    This allows us to prioritize on the worker side.
    """

    def __init__(self):
        def compute_td_error(
            obs_t, act_t, rew_t, obs_tp1, terminateds_mask, importance_weights
        ):
            input_dict = self._lazy_tensor_dict({SampleBatch.CUR_OBS: obs_t})
            input_dict[SampleBatch.ACTIONS] = act_t
            input_dict[SampleBatch.REWARDS] = rew_t
            input_dict[SampleBatch.NEXT_OBS] = obs_tp1
            input_dict[SampleBatch.TERMINATEDS] = terminateds_mask
            input_dict[PRIO_WEIGHTS] = importance_weights

            # Do forward pass on loss to update td error attribute
            build_q_losses(self, self.model, None, input_dict)

            return self.model.tower_stats["q_loss"].td_error

        self.compute_td_error = compute_td_error


def build_q_model_and_distribution(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> Tuple[ModelV2, TorchDistributionWrapper]:
    """Build q_model and target_model for DQN

    Args:
        policy: The policy, which will use the model for optimization.
        obs_space (gym.spaces.Space): The policy's observation space.
        action_space (gym.spaces.Space): The policy's action space.
        config (AlgorithmConfigDict):

    Returns:
        (q_model, TorchCategorical)
            Note: The target q model will not be returned, just assigned to
            `policy.target_model`.
    """
    if not isinstance(action_space, gym.spaces.Discrete):
        raise UnsupportedSpaceException(
            "Action space {} is not supported for DQN.".format(action_space)
        )

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
        or config["exploration_config"]["type"] == "ParameterNoise"
    )

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
        add_layer_norm=add_layer_norm,
    )

    policy.target_model = ModelCatalog.get_model_v2(
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
        add_layer_norm=add_layer_norm,
    )

    # Return a Torch TorchCategorical distribution where the temperature
    # parameter is partially binded to the configured value.
    temperature = config["categorical_distribution_temperature"]

    return model, get_torch_categorical_class_with_temperature(temperature)


def get_distribution_inputs_and_class(
    policy: Policy,
    model: ModelV2,
    input_dict: SampleBatch,
    *,
    explore: bool = True,
    is_training: bool = False,
    **kwargs
) -> Tuple[TensorType, type, List[TensorType]]:
    q_vals = compute_q_values(
        policy, model, input_dict, explore=explore, is_training=is_training
    )
    q_vals = q_vals[0] if isinstance(q_vals, tuple) else q_vals

    model.tower_stats["q_values"] = q_vals

    # Return a Torch TorchCategorical distribution where the temperature
    # parameter is partially binded to the configured value.
    temperature = policy.config["categorical_distribution_temperature"]

    return (
        q_vals,
        get_torch_categorical_class_with_temperature(temperature),
        [],  # state-out
    )


def build_q_losses(policy: Policy, model, _, train_batch: SampleBatch) -> TensorType:
    """Constructs the loss for DQNTorchPolicy.

    Args:
        policy: The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        train_batch: The training data.

    Returns:
        TensorType: A single loss tensor.
    """

    config = policy.config
    # Q-network evaluation.
    q_t, q_logits_t, q_probs_t, _ = compute_q_values(
        policy,
        model,
        {"obs": train_batch[SampleBatch.CUR_OBS]},
        explore=False,
        is_training=True,
    )

    # Target Q-network evaluation.
    q_tp1, q_logits_tp1, q_probs_tp1, _ = compute_q_values(
        policy,
        policy.target_models[model],
        {"obs": train_batch[SampleBatch.NEXT_OBS]},
        explore=False,
        is_training=True,
    )

    # Q scores for actions which we know were selected in the given state.
    one_hot_selection = F.one_hot(
        train_batch[SampleBatch.ACTIONS].long(), policy.action_space.n
    )
    q_t_selected = torch.sum(
        torch.where(q_t > FLOAT_MIN, q_t, torch.tensor(0.0, device=q_t.device))
        * one_hot_selection,
        1,
    )
    q_logits_t_selected = torch.sum(
        q_logits_t * torch.unsqueeze(one_hot_selection, -1), 1
    )

    # compute estimate of best possible value starting from state at t + 1
    if config["double_q"]:
        (
            q_tp1_using_online_net,
            q_logits_tp1_using_online_net,
            q_dist_tp1_using_online_net,
            _,
        ) = compute_q_values(
            policy,
            model,
            {"obs": train_batch[SampleBatch.NEXT_OBS]},
            explore=False,
            is_training=True,
        )
        q_tp1_best_using_online_net = torch.argmax(q_tp1_using_online_net, 1)
        q_tp1_best_one_hot_selection = F.one_hot(
            q_tp1_best_using_online_net, policy.action_space.n
        )
        q_tp1_best = torch.sum(
            torch.where(
                q_tp1 > FLOAT_MIN, q_tp1, torch.tensor(0.0, device=q_tp1.device)
            )
            * q_tp1_best_one_hot_selection,
            1,
        )
        q_probs_tp1_best = torch.sum(
            q_probs_tp1 * torch.unsqueeze(q_tp1_best_one_hot_selection, -1), 1
        )
    else:
        q_tp1_best_one_hot_selection = F.one_hot(
            torch.argmax(q_tp1, 1), policy.action_space.n
        )
        q_tp1_best = torch.sum(
            torch.where(
                q_tp1 > FLOAT_MIN, q_tp1, torch.tensor(0.0, device=q_tp1.device)
            )
            * q_tp1_best_one_hot_selection,
            1,
        )
        q_probs_tp1_best = torch.sum(
            q_probs_tp1 * torch.unsqueeze(q_tp1_best_one_hot_selection, -1), 1
        )

    loss_fn = huber_loss if policy.config["td_error_loss_fn"] == "huber" else l2_loss

    q_loss = QLoss(
        q_t_selected,
        q_logits_t_selected,
        q_tp1_best,
        q_probs_tp1_best,
        train_batch[PRIO_WEIGHTS],
        train_batch[SampleBatch.REWARDS],
        train_batch[SampleBatch.TERMINATEDS].float(),
        config["gamma"],
        config["n_step"],
        config["num_atoms"],
        config["v_min"],
        config["v_max"],
        loss_fn,
    )

    # Store values for stats function in model (tower), such that for
    # multi-GPU, we do not override them during the parallel loss phase.
    model.tower_stats["td_error"] = q_loss.td_error
    # TD-error tensor in final stats
    # will be concatenated and retrieved for each individual batch item.
    model.tower_stats["q_loss"] = q_loss

    return q_loss.loss


def adam_optimizer(
    policy: Policy, config: AlgorithmConfigDict
) -> "torch.optim.Optimizer":

    # By this time, the models have been moved to the GPU - if any - and we
    # can define our optimizers using the correct CUDA variables.
    if not hasattr(policy, "q_func_vars"):
        policy.q_func_vars = policy.model.variables()

    return torch.optim.Adam(
        policy.q_func_vars, lr=policy.cur_lr, eps=config["adam_epsilon"]
    )


def build_q_stats(policy: Policy, batch) -> Dict[str, TensorType]:
    stats = {}
    for stats_key in policy.model_gpu_towers[0].tower_stats["q_loss"].stats.keys():
        stats[stats_key] = torch.mean(
            torch.stack(
                [
                    t.tower_stats["q_loss"].stats[stats_key].to(policy.device)
                    for t in policy.model_gpu_towers
                    if "q_loss" in t.tower_stats
                ]
            )
        )
    stats["cur_lr"] = policy.cur_lr
    return stats


def setup_early_mixins(
    policy: Policy, obs_space, action_space, config: AlgorithmConfigDict
) -> None:
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])


def before_loss_init(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> None:
    ComputeTDErrorMixin.__init__(policy)
    TargetNetworkMixin.__init__(policy)


def compute_q_values(
    policy: Policy,
    model: ModelV2,
    input_dict,
    state_batches=None,
    seq_lens=None,
    explore=None,
    is_training: bool = False,
):
    config = policy.config

    model_out, state = model(input_dict, state_batches or [], seq_lens)

    if config["num_atoms"] > 1:
        (
            action_scores,
            z,
            support_logits_per_action,
            logits,
            probs_or_logits,
        ) = model.get_q_value_distributions(model_out)
    else:
        (action_scores, logits, probs_or_logits) = model.get_q_value_distributions(
            model_out
        )

    if config["dueling"]:
        state_score = model.get_state_value(model_out)
        if policy.config["num_atoms"] > 1:
            support_logits_per_action_mean = torch.mean(
                support_logits_per_action, dim=1
            )
            support_logits_per_action_centered = (
                support_logits_per_action
                - torch.unsqueeze(support_logits_per_action_mean, dim=1)
            )
            support_logits_per_action = (
                torch.unsqueeze(state_score, dim=1) + support_logits_per_action_centered
            )
            support_prob_per_action = nn.functional.softmax(
                support_logits_per_action, dim=-1
            )
            value = torch.sum(z * support_prob_per_action, dim=-1)
            logits = support_logits_per_action
            probs_or_logits = support_prob_per_action
        else:
            advantages_mean = reduce_mean_ignore_inf(action_scores, 1)
            advantages_centered = action_scores - torch.unsqueeze(advantages_mean, 1)
            value = state_score + advantages_centered
    else:
        value = action_scores

    return value, logits, probs_or_logits, state


def grad_process_and_td_error_fn(
    policy: Policy, optimizer: "torch.optim.Optimizer", loss: TensorType
) -> Dict[str, TensorType]:
    # Clip grads if configured.
    return apply_grad_clipping(policy, optimizer, loss)


def extra_action_out_fn(
    policy: Policy, input_dict, state_batches, model, action_dist
) -> Dict[str, TensorType]:
    return {"q_values": model.tower_stats["q_values"]}


DQNTorchPolicy = build_policy_class(
    name="DQNTorchPolicy",
    framework="torch",
    loss_fn=build_q_losses,
    get_default_config=lambda: ray.rllib.algorithms.dqn.dqn.DQNConfig(),
    make_model_and_action_dist=build_q_model_and_distribution,
    action_distribution_fn=get_distribution_inputs_and_class,
    stats_fn=build_q_stats,
    postprocess_fn=postprocess_nstep_and_prio,
    optimizer_fn=adam_optimizer,
    extra_grad_process_fn=grad_process_and_td_error_fn,
    extra_learn_fetches_fn=concat_multi_gpu_td_errors,
    extra_action_out_fn=extra_action_out_fn,
    before_init=setup_early_mixins,
    before_loss_init=before_loss_init,
    mixins=[
        TargetNetworkMixin,
        ComputeTDErrorMixin,
        LearningRateSchedule,
    ],
)
