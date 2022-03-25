"""PyTorch policy class used for Simple Q-Learning"""

import logging
from typing import Dict, Tuple

import gym
import ray
from ray.rllib.agents.dqn.simple_q_tf_policy import (
    build_q_models,
    compute_q_values,
    get_distribution_inputs_and_class,
)
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import (
    TorchCategorical,
    TorchDistributionWrapper,
)
from ray.rllib.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import concat_multi_gpu_td_errors, huber_loss
from ray.rllib.utils.typing import TensorType, TrainerConfigDict

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional
logger = logging.getLogger(__name__)


class TargetNetworkMixin:
    """Assign the `update_target` method to the SimpleQTorchPolicy

    The function is called every `target_network_update_freq` steps by the
    master learner.
    """

    def __init__(self):
        # Hard initial update from Q-net(s) to target Q-net(s).
        self.update_target()

    def update_target(self):
        # Update_target_fn will be called periodically to copy Q network to
        # target Q networks.
        state_dict = self.model.state_dict()
        for target in self.target_models.values():
            target.load_state_dict(state_dict)

    @override(TorchPolicy)
    def set_weights(self, weights):
        # Makes sure that whenever we restore weights for this policy's
        # model, we sync the target network (from the main model)
        # at the same time.
        TorchPolicy.set_weights(self, weights)
        self.update_target()


def build_q_model_and_distribution(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> Tuple[ModelV2, TorchDistributionWrapper]:
    return build_q_models(policy, obs_space, action_space, config), TorchCategorical


def build_q_losses(
    policy: Policy, model, dist_class, train_batch: SampleBatch
) -> TensorType:
    """Constructs the loss for SimpleQTorchPolicy.

    Args:
        policy (Policy): The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        dist_class (Type[ActionDistribution]): The action distribution class.
        train_batch (SampleBatch): The training data.

    Returns:
        TensorType: A single loss tensor.
    """
    target_model = policy.target_models[model]

    # q network evaluation
    q_t = compute_q_values(
        policy, model, train_batch[SampleBatch.CUR_OBS], explore=False, is_training=True
    )

    # target q network evalution
    q_tp1 = compute_q_values(
        policy,
        target_model,
        train_batch[SampleBatch.NEXT_OBS],
        explore=False,
        is_training=True,
    )

    # q scores for actions which we know were selected in the given state.
    one_hot_selection = F.one_hot(
        train_batch[SampleBatch.ACTIONS].long(), policy.action_space.n
    )
    q_t_selected = torch.sum(q_t * one_hot_selection, 1)

    # compute estimate of best possible value starting from state at t + 1
    dones = train_batch[SampleBatch.DONES].float()
    q_tp1_best_one_hot_selection = F.one_hot(
        torch.argmax(q_tp1, 1), policy.action_space.n
    )
    q_tp1_best = torch.sum(q_tp1 * q_tp1_best_one_hot_selection, 1)
    q_tp1_best_masked = (1.0 - dones) * q_tp1_best

    # compute RHS of bellman equation
    q_t_selected_target = (
        train_batch[SampleBatch.REWARDS] + policy.config["gamma"] * q_tp1_best_masked
    )

    # Compute the error (Square/Huber).
    td_error = q_t_selected - q_t_selected_target.detach()
    loss = torch.mean(huber_loss(td_error))

    # Store values for stats function in model (tower), such that for
    # multi-GPU, we do not override them during the parallel loss phase.
    model.tower_stats["loss"] = loss
    # TD-error tensor in final stats
    # will be concatenated and retrieved for each individual batch item.
    model.tower_stats["td_error"] = td_error

    return loss


def stats_fn(policy: Policy, batch: SampleBatch) -> Dict[str, TensorType]:
    return {"loss": torch.mean(torch.stack(policy.get_tower_stats("loss")))}


def extra_action_out_fn(
    policy: Policy, input_dict, state_batches, model, action_dist
) -> Dict[str, TensorType]:
    """Adds q-values to the action out dict."""
    return {"q_values": policy.q_values}


def setup_late_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> None:
    """Call all mixin classes' constructors before SimpleQTorchPolicy
    initialization.

    Args:
        policy (Policy): The Policy object.
        obs_space (gym.spaces.Space): The Policy's observation space.
        action_space (gym.spaces.Space): The Policy's action space.
        config (TrainerConfigDict): The Policy's config.
    """
    TargetNetworkMixin.__init__(policy)


SimpleQTorchPolicy = build_policy_class(
    name="SimpleQPolicy",
    framework="torch",
    loss_fn=build_q_losses,
    get_default_config=lambda: ray.rllib.agents.dqn.simple_q.DEFAULT_CONFIG,
    stats_fn=stats_fn,
    extra_action_out_fn=extra_action_out_fn,
    after_init=setup_late_mixins,
    make_model_and_action_dist=build_q_model_and_distribution,
    mixins=[TargetNetworkMixin],
    action_distribution_fn=get_distribution_inputs_and_class,
    extra_learn_fetches_fn=concat_multi_gpu_td_errors,
)
