"""
PyTorch policy class used for CQL.
"""
import numpy as np
import gymnasium as gym
import logging
import tree
from typing import Dict, List, Tuple, Type, Union

import ray
from ray.rllib.algorithms.sac.sac_tf_policy import (
    postprocess_trajectory,
    validate_spaces,
)
from ray.rllib.algorithms.sac.sac_torch_policy import (
    _get_dist_class,
    stats,
    build_sac_model_and_action_dist,
    optimizer_fn,
    ComputeTDErrorMixin,
    setup_late_mixins,
    action_distribution_fn,
)
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.torch_mixins import TargetNetworkMixin
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.typing import LocalOptimizer, TensorType, AlgorithmConfigDict
from ray.rllib.utils.torch_utils import (
    apply_grad_clipping,
    convert_to_torch_tensor,
    concat_multi_gpu_td_errors,
)

torch, nn = try_import_torch()
F = nn.functional

logger = logging.getLogger(__name__)

MEAN_MIN = -9.0
MEAN_MAX = 9.0


def _repeat_tensor(t: TensorType, n: int):
    # Insert new dimension at posotion 1 into tensor t
    t_rep = t.unsqueeze(1)
    # Repeat tensor t_rep along new dimension n times
    t_rep = torch.repeat_interleave(t_rep, n, dim=1)
    # Merge new dimension into batch dimension
    t_rep = t_rep.view(-1, *t.shape[1:])
    return t_rep


# Returns policy tiled actions and log probabilities for CQL Loss
def policy_actions_repeat(model, action_dist, obs, num_repeat=1):
    batch_size = tree.flatten(obs)[0].shape[0]
    obs_temp = tree.map_structure(lambda t: _repeat_tensor(t, num_repeat), obs)
    logits, _ = model.get_action_model_outputs(obs_temp)
    policy_dist = action_dist(logits, model)
    actions, logp_ = policy_dist.sample_logp()
    logp = logp_.unsqueeze(-1)
    return actions, logp.view(batch_size, num_repeat, 1)


def q_values_repeat(model, obs, actions, twin=False):
    action_shape = actions.shape[0]
    obs_shape = tree.flatten(obs)[0].shape[0]
    num_repeat = int(action_shape / obs_shape)
    obs_temp = tree.map_structure(lambda t: _repeat_tensor(t, num_repeat), obs)
    if not twin:
        preds_, _ = model.get_q_values(obs_temp, actions)
    else:
        preds_, _ = model.get_twin_q_values(obs_temp, actions)
    preds = preds_.view(obs_shape, num_repeat, 1)
    return preds


def cql_loss(
    policy: Policy,
    model: ModelV2,
    dist_class: Type[TorchDistributionWrapper],
    train_batch: SampleBatch,
) -> Union[TensorType, List[TensorType]]:
    logger.info(f"Current iteration = {policy.cur_iter}")
    policy.cur_iter += 1

    # Look up the target model (tower) using the model tower.
    target_model = policy.target_models[model]

    # For best performance, turn deterministic off
    deterministic = policy.config["_deterministic_loss"]
    assert not deterministic
    twin_q = policy.config["twin_q"]
    discount = policy.config["gamma"]
    action_low = model.action_space.low[0]
    action_high = model.action_space.high[0]

    # CQL Parameters
    bc_iters = policy.config["bc_iters"]
    cql_temp = policy.config["temperature"]
    num_actions = policy.config["num_actions"]
    min_q_weight = policy.config["min_q_weight"]
    use_lagrange = policy.config["lagrangian"]
    target_action_gap = policy.config["lagrangian_thresh"]

    obs = train_batch[SampleBatch.CUR_OBS]
    actions = train_batch[SampleBatch.ACTIONS]
    rewards = train_batch[SampleBatch.REWARDS].float()
    next_obs = train_batch[SampleBatch.NEXT_OBS]
    terminals = train_batch[SampleBatch.TERMINATEDS]

    model_out_t, _ = model(SampleBatch(obs=obs, _is_training=True), [], None)

    model_out_tp1, _ = model(SampleBatch(obs=next_obs, _is_training=True), [], None)

    target_model_out_tp1, _ = target_model(
        SampleBatch(obs=next_obs, _is_training=True), [], None
    )

    action_dist_class = _get_dist_class(policy, policy.config, policy.action_space)
    action_dist_inputs_t, _ = model.get_action_model_outputs(model_out_t)
    action_dist_t = action_dist_class(action_dist_inputs_t, model)
    policy_t, log_pis_t = action_dist_t.sample_logp()
    log_pis_t = torch.unsqueeze(log_pis_t, -1)

    # Unlike original SAC, Alpha and Actor Loss are computed first.
    # Alpha Loss
    alpha_loss = -(model.log_alpha * (log_pis_t + model.target_entropy).detach()).mean()

    batch_size = tree.flatten(obs)[0].shape[0]
    if batch_size == policy.config["train_batch_size"]:
        policy.alpha_optim.zero_grad()
        alpha_loss.backward()
        policy.alpha_optim.step()

    # Policy Loss (Either Behavior Clone Loss or SAC Loss)
    alpha = torch.exp(model.log_alpha)
    if policy.cur_iter >= bc_iters:
        min_q, _ = model.get_q_values(model_out_t, policy_t)
        if twin_q:
            twin_q_, _ = model.get_twin_q_values(model_out_t, policy_t)
            min_q = torch.min(min_q, twin_q_)
        actor_loss = (alpha.detach() * log_pis_t - min_q).mean()
    else:
        bc_logp = action_dist_t.logp(actions)
        actor_loss = (alpha.detach() * log_pis_t - bc_logp).mean()
        # actor_loss = -bc_logp.mean()

    if batch_size == policy.config["train_batch_size"]:
        policy.actor_optim.zero_grad()
        actor_loss.backward(retain_graph=True)
        policy.actor_optim.step()

    # Critic Loss (Standard SAC Critic L2 Loss + CQL Entropy Loss)
    # SAC Loss:
    # Q-values for the batched actions.
    action_dist_inputs_tp1, _ = model.get_action_model_outputs(model_out_tp1)
    action_dist_tp1 = action_dist_class(action_dist_inputs_tp1, model)
    policy_tp1, _ = action_dist_tp1.sample_logp()

    q_t, _ = model.get_q_values(model_out_t, train_batch[SampleBatch.ACTIONS])
    q_t_selected = torch.squeeze(q_t, dim=-1)
    if twin_q:
        twin_q_t, _ = model.get_twin_q_values(
            model_out_t, train_batch[SampleBatch.ACTIONS]
        )
        twin_q_t_selected = torch.squeeze(twin_q_t, dim=-1)

    # Target q network evaluation.
    q_tp1, _ = target_model.get_q_values(target_model_out_tp1, policy_tp1)
    if twin_q:
        twin_q_tp1, _ = target_model.get_twin_q_values(target_model_out_tp1, policy_tp1)
        # Take min over both twin-NNs.
        q_tp1 = torch.min(q_tp1, twin_q_tp1)

    q_tp1_best = torch.squeeze(input=q_tp1, dim=-1)
    q_tp1_best_masked = (1.0 - terminals.float()) * q_tp1_best

    # compute RHS of bellman equation
    q_t_target = (
        rewards + (discount ** policy.config["n_step"]) * q_tp1_best_masked
    ).detach()

    # Compute the TD-error (potentially clipped), for priority replay buffer
    base_td_error = torch.abs(q_t_selected - q_t_target)
    if twin_q:
        twin_td_error = torch.abs(twin_q_t_selected - q_t_target)
        td_error = 0.5 * (base_td_error + twin_td_error)
    else:
        td_error = base_td_error

    critic_loss_1 = nn.functional.mse_loss(q_t_selected, q_t_target)
    if twin_q:
        critic_loss_2 = nn.functional.mse_loss(twin_q_t_selected, q_t_target)

    # CQL Loss (We are using Entropy version of CQL (the best version))
    rand_actions = convert_to_torch_tensor(
        torch.FloatTensor(actions.shape[0] * num_actions, actions.shape[-1]).uniform_(
            action_low, action_high
        ),
        policy.device,
    )
    curr_actions, curr_logp = policy_actions_repeat(
        model, action_dist_class, model_out_t, num_actions
    )
    next_actions, next_logp = policy_actions_repeat(
        model, action_dist_class, model_out_tp1, num_actions
    )

    q1_rand = q_values_repeat(model, model_out_t, rand_actions)
    q1_curr_actions = q_values_repeat(model, model_out_t, curr_actions)
    q1_next_actions = q_values_repeat(model, model_out_t, next_actions)

    if twin_q:
        q2_rand = q_values_repeat(model, model_out_t, rand_actions, twin=True)
        q2_curr_actions = q_values_repeat(model, model_out_t, curr_actions, twin=True)
        q2_next_actions = q_values_repeat(model, model_out_t, next_actions, twin=True)

    random_density = np.log(0.5 ** curr_actions.shape[-1])
    cat_q1 = torch.cat(
        [
            q1_rand - random_density,
            q1_next_actions - next_logp.detach(),
            q1_curr_actions - curr_logp.detach(),
        ],
        1,
    )
    if twin_q:
        cat_q2 = torch.cat(
            [
                q2_rand - random_density,
                q2_next_actions - next_logp.detach(),
                q2_curr_actions - curr_logp.detach(),
            ],
            1,
        )

    min_qf1_loss_ = (
        torch.logsumexp(cat_q1 / cql_temp, dim=1).mean() * min_q_weight * cql_temp
    )
    min_qf1_loss = min_qf1_loss_ - (q_t.mean() * min_q_weight)
    if twin_q:
        min_qf2_loss_ = (
            torch.logsumexp(cat_q2 / cql_temp, dim=1).mean() * min_q_weight * cql_temp
        )
        min_qf2_loss = min_qf2_loss_ - (twin_q_t.mean() * min_q_weight)

    if use_lagrange:
        alpha_prime = torch.clamp(model.log_alpha_prime.exp(), min=0.0, max=1000000.0)[
            0
        ]
        min_qf1_loss = alpha_prime * (min_qf1_loss - target_action_gap)
        if twin_q:
            min_qf2_loss = alpha_prime * (min_qf2_loss - target_action_gap)
            alpha_prime_loss = 0.5 * (-min_qf1_loss - min_qf2_loss)
        else:
            alpha_prime_loss = -min_qf1_loss

    cql_loss = [min_qf1_loss]
    if twin_q:
        cql_loss.append(min_qf2_loss)

    critic_loss = [critic_loss_1 + min_qf1_loss]
    if twin_q:
        critic_loss.append(critic_loss_2 + min_qf2_loss)

    if batch_size == policy.config["train_batch_size"]:
        policy.critic_optims[0].zero_grad()
        critic_loss[0].backward(retain_graph=True)
        policy.critic_optims[0].step()

        if twin_q:
            policy.critic_optims[1].zero_grad()
            critic_loss[1].backward(retain_graph=False)
            policy.critic_optims[1].step()

    # Store values for stats function in model (tower), such that for
    # multi-GPU, we do not override them during the parallel loss phase.
    # SAC stats.
    model.tower_stats["q_t"] = q_t_selected
    model.tower_stats["policy_t"] = policy_t
    model.tower_stats["log_pis_t"] = log_pis_t
    model.tower_stats["actor_loss"] = actor_loss
    model.tower_stats["critic_loss"] = critic_loss
    model.tower_stats["alpha_loss"] = alpha_loss
    model.tower_stats["log_alpha_value"] = model.log_alpha
    model.tower_stats["alpha_value"] = alpha
    model.tower_stats["target_entropy"] = model.target_entropy
    # CQL stats.
    model.tower_stats["cql_loss"] = cql_loss

    # TD-error tensor in final stats
    # will be concatenated and retrieved for each individual batch item.
    model.tower_stats["td_error"] = td_error

    if use_lagrange:
        model.tower_stats["log_alpha_prime_value"] = model.log_alpha_prime[0]
        model.tower_stats["alpha_prime_value"] = alpha_prime
        model.tower_stats["alpha_prime_loss"] = alpha_prime_loss

        if batch_size == policy.config["train_batch_size"]:
            policy.alpha_prime_optim.zero_grad()
            alpha_prime_loss.backward()
            policy.alpha_prime_optim.step()

    # Return all loss terms corresponding to our optimizers.
    return tuple(
        [actor_loss]
        + critic_loss
        + [alpha_loss]
        + ([alpha_prime_loss] if use_lagrange else [])
    )


def cql_stats(policy: Policy, train_batch: SampleBatch) -> Dict[str, TensorType]:
    # Get SAC loss stats.
    stats_dict = stats(policy, train_batch)

    # Add CQL loss stats to the dict.
    stats_dict["cql_loss"] = torch.mean(
        torch.stack(*policy.get_tower_stats("cql_loss"))
    )

    if policy.config["lagrangian"]:
        stats_dict["log_alpha_prime_value"] = torch.mean(
            torch.stack(policy.get_tower_stats("log_alpha_prime_value"))
        )
        stats_dict["alpha_prime_value"] = torch.mean(
            torch.stack(policy.get_tower_stats("alpha_prime_value"))
        )
        stats_dict["alpha_prime_loss"] = torch.mean(
            torch.stack(policy.get_tower_stats("alpha_prime_loss"))
        )
    return stats_dict


def cql_optimizer_fn(
    policy: Policy, config: AlgorithmConfigDict
) -> Tuple[LocalOptimizer]:
    policy.cur_iter = 0
    opt_list = optimizer_fn(policy, config)
    if config["lagrangian"]:
        log_alpha_prime = nn.Parameter(torch.zeros(1, requires_grad=True).float())
        policy.model.register_parameter("log_alpha_prime", log_alpha_prime)
        policy.alpha_prime_optim = torch.optim.Adam(
            params=[policy.model.log_alpha_prime],
            lr=config["optimization"]["critic_learning_rate"],
            eps=1e-7,  # to match tf.keras.optimizers.Adam's epsilon default
        )
        return tuple(
            [policy.actor_optim]
            + policy.critic_optims
            + [policy.alpha_optim]
            + [policy.alpha_prime_optim]
        )
    return opt_list


def cql_setup_late_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> None:
    setup_late_mixins(policy, obs_space, action_space, config)
    if config["lagrangian"]:
        policy.model.log_alpha_prime = policy.model.log_alpha_prime.to(policy.device)


def compute_gradients_fn(policy, postprocessed_batch):
    batches = [policy._lazy_tensor_dict(postprocessed_batch)]
    model = policy.model
    policy._loss(policy, model, policy.dist_class, batches[0])
    stats = {LEARNER_STATS_KEY: policy._convert_to_numpy(cql_stats(policy, batches[0]))}
    return [None, stats]


def apply_gradients_fn(policy, gradients):
    return


# Build a child class of `TorchPolicy`, given the custom functions defined
# above.
CQLTorchPolicy = build_policy_class(
    name="CQLTorchPolicy",
    framework="torch",
    loss_fn=cql_loss,
    get_default_config=lambda: ray.rllib.algorithms.cql.cql.CQLConfig(),
    stats_fn=cql_stats,
    postprocess_fn=postprocess_trajectory,
    extra_grad_process_fn=apply_grad_clipping,
    optimizer_fn=cql_optimizer_fn,
    validate_spaces=validate_spaces,
    before_loss_init=cql_setup_late_mixins,
    make_model_and_action_dist=build_sac_model_and_action_dist,
    extra_learn_fetches_fn=concat_multi_gpu_td_errors,
    mixins=[TargetNetworkMixin, ComputeTDErrorMixin],
    action_distribution_fn=action_distribution_fn,
    compute_gradients_fn=compute_gradients_fn,
    apply_gradients_fn=apply_gradients_fn,
)
