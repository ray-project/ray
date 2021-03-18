import gym
from typing import Optional, Dict

import ray
from ray.rllib.agents.ppo.ppo_torch_policy import ValueNetworkMixin
from ray.rllib.evaluation.postprocessing import compute_gae_for_sample_batch, \
    Postprocessing
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_ops import apply_grad_clipping
from ray.rllib.utils.typing import TrainerConfigDict, TensorType, \
    PolicyID, LocalOptimizer
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.evaluation import MultiAgentEpisode

torch, nn = try_import_torch()


def add_advantages(policy: Policy,
                   sample_batch: SampleBatch,
                   other_agent_batches: Optional[Dict[PolicyID, SampleBatch]]=None,
                   episode: Optional[MultiAgentEpisode]=None) -> SampleBatch:

    # Stub serving backward compatibility.
    deprecation_warning(
        old="rllib.agents.a3c.a3c_torch_policy.add_advantages",
        new="rllib.evaluation.postprocessing.compute_gae_for_sample_batch",
        error=False)

    return compute_gae_for_sample_batch(policy, sample_batch,
                                        other_agent_batches, episode)


def actor_critic_loss(policy: Policy, model: ModelV2, dist_class: ActionDistribution, train_batch: SampleBatch) -> TensorType:
    logits, _ = model.from_batch(train_batch)
    values = model.value_function()
    dist = dist_class(logits, model)
    log_probs = dist.logp(train_batch[SampleBatch.ACTIONS])
    policy.entropy = dist.entropy().sum()
    policy.pi_err = -train_batch[Postprocessing.ADVANTAGES].dot(
        log_probs.reshape(-1))
    policy.value_err = torch.sum(
        torch.pow(
            values.reshape(-1) - train_batch[Postprocessing.VALUE_TARGETS],
            2.0))
    overall_err = sum([
        policy.pi_err,
        policy.config["vf_loss_coeff"] * policy.value_err,
        -policy.config["entropy_coeff"] * policy.entropy,
    ])
    return overall_err


def loss_and_entropy_stats(policy: Policy, train_batch: SampleBatch) -> Dict[str, TensorType]:
    return {
        "policy_entropy": policy.entropy.item(),
        "policy_loss": policy.pi_err.item(),
        "vf_loss": policy.value_err.item(),
    }


def model_value_predictions(policy: Policy, input_dict: Dict[str, TensorType], state_batches, model: ModelV2,
                            action_dist; ActionDistribution) -> Dict[str, TensorType]:
    return {SampleBatch.VF_PREDS: model.value_function()}


def torch_optimizer(policy: Policy, config: TrainerConfigDict) -> LocalOptimizer:
    return torch.optim.Adam(policy.model.parameters(), lr=config["lr"])


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


A3CTorchPolicy = build_policy_class(
    name="A3CTorchPolicy",
    framework="torch",
    get_default_config=lambda: ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG,
    loss_fn=actor_critic_loss,
    stats_fn=loss_and_entropy_stats,
    postprocess_fn=compute_gae_for_sample_batch,
    extra_action_out_fn=model_value_predictions,
    extra_grad_process_fn=apply_grad_clipping,
    optimizer_fn=torch_optimizer,
    before_loss_init=setup_mixins,
    mixins=[ValueNetworkMixin],
)
