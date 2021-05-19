import gym
from typing import Dict

import ray
from ray.rllib.agents.ppo.ppo_torch_policy import ValueNetworkMixin
from ray.rllib.agents.marwil.marwil_tf_policy import postprocess_advantages
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_ops import apply_grad_clipping, explained_variance
from ray.rllib.utils.typing import TrainerConfigDict, TensorType
from ray.rllib.policy.policy import Policy
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2

torch, _ = try_import_torch()


def marwil_loss(policy: Policy, model: ModelV2, dist_class: ActionDistribution,
                train_batch: SampleBatch) -> TensorType:
    model_out, _ = model.from_batch(train_batch)
    action_dist = dist_class(model_out, model)
    state_values = model.value_function()
    advantages = train_batch[Postprocessing.ADVANTAGES]
    actions = train_batch[SampleBatch.ACTIONS]

    # Advantage estimation.
    adv = advantages - state_values
    adv_squared = torch.mean(torch.pow(adv, 2.0))

    # Value loss.
    policy.v_loss = 0.5 * adv_squared

    # Policy loss.
    # Update averaged advantage norm.
    policy.ma_adv_norm.add_(1e-6 * (adv_squared - policy.ma_adv_norm))
    # Exponentially weighted advantages.
    exp_advs = torch.exp(policy.config["beta"] *
                         (adv / (1e-8 + torch.pow(policy.ma_adv_norm, 0.5))))
    # log\pi_\theta(a|s)
    logprobs = action_dist.logp(actions)
    policy.p_loss = -1.0 * torch.mean(exp_advs.detach() * logprobs)

    # Combine both losses.
    policy.total_loss = policy.p_loss + policy.config["vf_coeff"] * \
        policy.v_loss
    explained_var = explained_variance(advantages, state_values)
    policy.explained_variance = torch.mean(explained_var)

    return policy.total_loss


def stats(policy: Policy, train_batch: SampleBatch) -> Dict[str, TensorType]:
    return {
        "policy_loss": policy.p_loss,
        "vf_loss": policy.v_loss,
        "total_loss": policy.total_loss,
        "vf_explained_var": policy.explained_variance,
    }


def setup_mixins(policy: Policy, obs_space: gym.spaces.Space,
                 action_space: gym.spaces.Space,
                 config: TrainerConfigDict) -> None:
    # Create a var.
    policy.ma_adv_norm = torch.tensor(
        [100.0], dtype=torch.float32, requires_grad=False).to(policy.device)
    # Setup Value branch of our NN.
    ValueNetworkMixin.__init__(policy, obs_space, action_space, config)


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
