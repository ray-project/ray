import gym
from typing import Dict

import ray
from ray.rllib.agents.a3c.a3c_torch_policy import ValueNetworkMixin
from ray.rllib.agents.marwil.marwil_tf_policy import postprocess_advantages
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import apply_grad_clipping, explained_variance
from ray.rllib.utils.typing import TrainerConfigDict, TensorType
from ray.rllib.policy.policy import Policy
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2

torch, _ = try_import_torch()


def marwil_loss(policy: Policy, model: ModelV2, dist_class: ActionDistribution,
                train_batch: SampleBatch) -> TensorType:
    model_out, _ = model(train_batch)
    action_dist = dist_class(model_out, model)
    actions = train_batch[SampleBatch.ACTIONS]
    # log\pi_\theta(a|s)
    logprobs = action_dist.logp(actions)

    # Advantage estimation.
    if policy.config["beta"] != 0.0:
        cumulative_rewards = train_batch[Postprocessing.ADVANTAGES]
        state_values = model.value_function()
        adv = cumulative_rewards - state_values
        adv_squared_mean = torch.mean(torch.pow(adv, 2.0))

        explained_var = explained_variance(cumulative_rewards, state_values)
        policy.explained_variance = torch.mean(explained_var)

        # Policy loss.
        # Update averaged advantage norm.
        rate = policy.config["moving_average_sqd_adv_norm_update_rate"]
        policy._moving_average_sqd_adv_norm.add_(
            rate * (adv_squared_mean - policy._moving_average_sqd_adv_norm))
        # Exponentially weighted advantages.
        exp_advs = torch.exp(
            policy.config["beta"] *
            (adv /
             (1e-8 + torch.pow(policy._moving_average_sqd_adv_norm, 0.5))))
        policy.p_loss = -torch.mean(exp_advs.detach() * logprobs)
        # Value loss.
        policy.v_loss = 0.5 * adv_squared_mean
    else:
        # Policy loss (simple BC loss term).
        policy.p_loss = -1.0 * torch.mean(logprobs)
        # Value loss.
        policy.v_loss = 0.0

    # Combine both losses.
    policy.total_loss = policy.p_loss + policy.config["vf_coeff"] * \
        policy.v_loss

    return policy.total_loss


def stats(policy: Policy, train_batch: SampleBatch) -> Dict[str, TensorType]:
    stats = {
        "policy_loss": policy.p_loss,
        "total_loss": policy.total_loss,
    }
    if policy.config["beta"] != 0.0:
        stats["moving_average_sqd_adv_norm"] = \
            policy._moving_average_sqd_adv_norm
        stats["vf_explained_var"] = policy.explained_variance
        stats["vf_loss"] = policy.v_loss

    return stats


def setup_mixins(policy: Policy, obs_space: gym.spaces.Space,
                 action_space: gym.spaces.Space,
                 config: TrainerConfigDict) -> None:
    # Setup Value branch of our NN.
    ValueNetworkMixin.__init__(policy, obs_space, action_space, config)

    # Not needed for pure BC.
    if policy.config["beta"] != 0.0:
        # Set up a torch-var for the squared moving avg. advantage norm.
        policy._moving_average_sqd_adv_norm = torch.tensor(
            [policy.config["moving_average_sqd_adv_norm_start"]],
            dtype=torch.float32,
            requires_grad=False).to(policy.device)


MARWILTorchPolicy = build_policy_class(
    name="MARWILTorchPolicy",
    framework="torch",
    loss_fn=marwil_loss,
    get_default_config=lambda: ray.rllib.agents.marwil.marwil.DEFAULT_CONFIG,
    stats_fn=stats,
    postprocess_fn=postprocess_advantages,
    extra_grad_process_fn=apply_grad_clipping,
    before_loss_init=setup_mixins,
    mixins=[ValueNetworkMixin])
