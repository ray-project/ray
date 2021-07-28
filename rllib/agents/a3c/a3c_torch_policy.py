import gym
from typing import Optional, Dict

import ray
from ray.rllib.agents.ppo.ppo_torch_policy import ValueNetworkMixin
from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.evaluation.postprocessing import compute_gae_for_sample_batch, \
    Postprocessing
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_ops import apply_grad_clipping, sequence_mask
from ray.rllib.utils.typing import TrainerConfigDict, TensorType, \
    PolicyID, LocalOptimizer

torch, nn = try_import_torch()


def add_advantages(
        policy: Policy,
        sample_batch: SampleBatch,
        other_agent_batches: Optional[Dict[PolicyID, SampleBatch]] = None,
        episode: Optional[MultiAgentEpisode] = None) -> SampleBatch:

    # Stub serving backward compatibility.
    deprecation_warning(
        old="rllib.agents.a3c.a3c_torch_policy.add_advantages",
        new="rllib.evaluation.postprocessing.compute_gae_for_sample_batch",
        error=False)

    return compute_gae_for_sample_batch(policy, sample_batch,
                                        other_agent_batches, episode)


def actor_critic_loss(policy: Policy, model: ModelV2,
                      dist_class: ActionDistribution,
                      train_batch: SampleBatch) -> TensorType:
    logits, _ = model.from_batch(train_batch)
    values = model.value_function()

    if policy.is_recurrent():
        max_seq_len = torch.max(train_batch["seq_lens"])
        mask_orig = sequence_mask(train_batch["seq_lens"], max_seq_len)
        valid_mask = torch.reshape(mask_orig, [-1])
    else:
        valid_mask = torch.ones_like(values, dtype=torch.bool)

    dist = dist_class(logits, model)
    log_probs = dist.logp(train_batch[SampleBatch.ACTIONS]).reshape(-1)
    policy.pi_err = -torch.sum(
        torch.masked_select(log_probs * train_batch[Postprocessing.ADVANTAGES],
                            valid_mask))

    # Compute a value function loss.
    if policy.config["use_critic"]:
        policy.value_err = 0.5 * torch.sum(
            torch.pow(
                torch.masked_select(
                    values.reshape(-1) -
                    train_batch[Postprocessing.VALUE_TARGETS], valid_mask),
                2.0))
    # Ignore the value function.
    else:
        policy.value_err = 0.0

    policy.entropy = torch.sum(torch.masked_select(dist.entropy(), valid_mask))

    total_loss = (
        policy.pi_err + policy.value_err * policy.config["vf_loss_coeff"] -
        policy.entropy * policy.config["entropy_coeff"])

    return total_loss


def loss_and_entropy_stats(policy: Policy,
                           train_batch: SampleBatch) -> Dict[str, TensorType]:
    return {
        "policy_entropy": policy.entropy,
        "policy_loss": policy.pi_err,
        "vf_loss": policy.value_err,
    }


def model_value_predictions(
        policy: Policy, input_dict: Dict[str, TensorType], state_batches,
        model: ModelV2,
        action_dist: ActionDistribution) -> Dict[str, TensorType]:
    return {SampleBatch.VF_PREDS: model.value_function()}


def torch_optimizer(policy: Policy,
                    config: TrainerConfigDict) -> LocalOptimizer:
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
