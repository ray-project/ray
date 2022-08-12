"""PyTorch policy class used for SlateQ."""

import gym
import logging
import numpy as np
from typing import Dict, Tuple, Type

import ray
from ray.rllib.algorithms.sac.sac_torch_policy import TargetNetworkMixin
from ray.rllib.algorithms.slateq.slateq_torch_model import SlateQTorchModel
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import (
    TorchCategorical,
    TorchDistributionWrapper,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import (
    apply_grad_clipping,
    concat_multi_gpu_td_errors,
    convert_to_torch_tensor,
    huber_loss,
)
from ray.rllib.utils.typing import TensorType, AlgorithmConfigDict

torch, nn = try_import_torch()
logger = logging.getLogger(__name__)


def build_slateq_model_and_distribution(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> Tuple[ModelV2, Type[TorchDistributionWrapper]]:
    """Build models for SlateQ

    Args:
        policy: The policy, which will use the model for optimization.
        obs_space: The policy's observation space.
        action_space: The policy's action space.
        config: The Algorithm's config dict.

    Returns:
        Tuple consisting of 1) Q-model and 2) an action distribution class.
    """
    model = SlateQTorchModel(
        obs_space,
        action_space,
        num_outputs=action_space.nvec[0],
        model_config=config["model"],
        name="slateq_model",
        fcnet_hiddens_per_candidate=config["fcnet_hiddens_per_candidate"],
    )

    policy.target_model = SlateQTorchModel(
        obs_space,
        action_space,
        num_outputs=action_space.nvec[0],
        model_config=config["model"],
        name="target_slateq_model",
        fcnet_hiddens_per_candidate=config["fcnet_hiddens_per_candidate"],
    )

    return model, TorchCategorical


def build_slateq_losses(
    policy: Policy,
    model: ModelV2,
    _,
    train_batch: SampleBatch,
) -> TensorType:
    """Constructs the choice- and Q-value losses for the SlateQTorchPolicy.

    Args:
        policy: The Policy to calculate the loss for.
        model: The Model to calculate the loss for.
        train_batch: The training data.

    Returns:
        The user-choice- and Q-value loss tensors.
    """

    # B=batch size
    # S=slate size
    # C=num candidates
    # E=embedding size
    # A=number of all possible slates

    # Q-value computations.
    # ---------------------
    # action.shape: [B, S]
    actions = train_batch[SampleBatch.ACTIONS]

    observation = convert_to_torch_tensor(
        train_batch[SampleBatch.OBS], device=actions.device
    )
    # user.shape: [B, E]
    user_obs = observation["user"]
    batch_size, embedding_size = user_obs.shape
    # doc.shape: [B, C, E]
    doc_obs = list(observation["doc"].values())

    A, S = policy.slates.shape

    # click_indicator.shape: [B, S]
    click_indicator = torch.stack(
        [k["click"] for k in observation["response"]], 1
    ).float()
    # item_reward.shape: [B, S]
    item_reward = torch.stack([k["watch_time"] for k in observation["response"]], 1)
    # q_values.shape: [B, C]
    q_values = model.get_q_values(user_obs, doc_obs)
    # slate_q_values.shape: [B, S]
    slate_q_values = torch.take_along_dim(q_values, actions.long(), dim=-1)
    # Only get the Q from the clicked document.
    # replay_click_q.shape: [B]
    replay_click_q = torch.sum(slate_q_values * click_indicator, dim=1)

    # Target computations.
    # --------------------
    next_obs = convert_to_torch_tensor(
        train_batch[SampleBatch.NEXT_OBS], device=actions.device
    )

    # user.shape: [B, E]
    user_next_obs = next_obs["user"]
    # doc.shape: [B, C, E]
    doc_next_obs = list(next_obs["doc"].values())
    # Only compute the watch time reward of the clicked item.
    reward = torch.sum(item_reward * click_indicator, dim=1)

    # TODO: Find out, whether it's correct here to use obs, not next_obs!
    # Dopamine uses obs, then next_obs only for the score.
    # next_q_values = policy.target_model.get_q_values(user_next_obs, doc_next_obs)
    next_q_values = policy.target_models[model].get_q_values(user_obs, doc_obs)
    scores, score_no_click = score_documents(user_next_obs, doc_next_obs)

    # next_q_values_slate.shape: [B, A, S]
    indices = policy.slates_indices.to(next_q_values.device)
    next_q_values_slate = torch.take_along_dim(next_q_values, indices, dim=1).reshape(
        [-1, A, S]
    )
    # scores_slate.shape [B, A, S]
    scores_slate = torch.take_along_dim(scores, indices, dim=1).reshape([-1, A, S])
    # score_no_click_slate.shape: [B, A]
    score_no_click_slate = torch.reshape(
        torch.tile(score_no_click, policy.slates.shape[:1]), [batch_size, -1]
    )

    # next_q_target_slate.shape: [B, A]
    next_q_target_slate = torch.sum(next_q_values_slate * scores_slate, dim=2) / (
        torch.sum(scores_slate, dim=2) + score_no_click_slate
    )
    next_q_target_max, _ = torch.max(next_q_target_slate, dim=1)

    target = reward + policy.config["gamma"] * next_q_target_max * (
        1.0 - train_batch["dones"].float()
    )
    target = target.detach()

    clicked = torch.sum(click_indicator, dim=1)
    mask_clicked_slates = clicked > 0
    clicked_indices = torch.arange(batch_size).to(mask_clicked_slates.device)
    clicked_indices = torch.masked_select(clicked_indices, mask_clicked_slates)
    # Clicked_indices is a vector and torch.gather selects the batch dimension.
    q_clicked = torch.gather(replay_click_q, 0, clicked_indices)
    target_clicked = torch.gather(target, 0, clicked_indices)

    td_error = torch.where(
        clicked.bool(),
        replay_click_q - target,
        torch.zeros_like(train_batch[SampleBatch.REWARDS]),
    )
    if policy.config["use_huber"]:
        loss = huber_loss(td_error, delta=policy.config["huber_threshold"])
    else:
        loss = torch.pow(td_error, 2.0)
    loss = torch.mean(loss)
    td_error = torch.abs(td_error)
    mean_td_error = torch.mean(td_error)

    # Store values for stats function in model (tower), such that for
    # multi-GPU, we do not override them during the parallel loss phase.
    model.tower_stats["q_values"] = torch.mean(q_values)
    model.tower_stats["q_clicked"] = torch.mean(q_clicked)
    model.tower_stats["scores"] = torch.mean(scores)
    model.tower_stats["score_no_click"] = torch.mean(score_no_click)
    model.tower_stats["slate_q_values"] = torch.mean(slate_q_values)
    model.tower_stats["replay_click_q"] = torch.mean(replay_click_q)
    model.tower_stats["bellman_reward"] = torch.mean(reward)
    model.tower_stats["next_q_values"] = torch.mean(next_q_values)
    model.tower_stats["target"] = torch.mean(target)
    model.tower_stats["next_q_target_slate"] = torch.mean(next_q_target_slate)
    model.tower_stats["next_q_target_max"] = torch.mean(next_q_target_max)
    model.tower_stats["target_clicked"] = torch.mean(target_clicked)
    model.tower_stats["q_loss"] = loss
    model.tower_stats["td_error"] = td_error
    model.tower_stats["mean_td_error"] = mean_td_error
    model.tower_stats["mean_actions"] = torch.mean(actions.float())

    # selected_doc.shape: [batch_size, slate_size, embedding_size]
    selected_doc = torch.gather(
        # input.shape: [batch_size, num_docs, embedding_size]
        torch.stack(doc_obs, 1),
        1,
        # index.shape: [batch_size, slate_size, embedding_size]
        actions.unsqueeze(2).expand(-1, -1, embedding_size).long(),
    )

    scores = model.choice_model(user_obs, selected_doc)

    # click_indicator.shape: [batch_size, slate_size]
    # no_clicks.shape: [batch_size, 1]
    no_clicks = 1 - torch.sum(click_indicator, 1, keepdim=True)
    # targets.shape: [batch_size, slate_size+1]
    targets = torch.cat([click_indicator, no_clicks], dim=1)
    choice_loss = nn.functional.cross_entropy(scores, torch.argmax(targets, dim=1))
    # print(model.choice_model.a.item(), model.choice_model.b.item())

    model.tower_stats["choice_loss"] = choice_loss

    return choice_loss, loss


def build_slateq_stats(policy: Policy, batch) -> Dict[str, TensorType]:
    stats = {
        "q_values": torch.mean(torch.stack(policy.get_tower_stats("q_values"))),
        "q_clicked": torch.mean(torch.stack(policy.get_tower_stats("q_clicked"))),
        "scores": torch.mean(torch.stack(policy.get_tower_stats("scores"))),
        "score_no_click": torch.mean(
            torch.stack(policy.get_tower_stats("score_no_click"))
        ),
        "slate_q_values": torch.mean(
            torch.stack(policy.get_tower_stats("slate_q_values"))
        ),
        "replay_click_q": torch.mean(
            torch.stack(policy.get_tower_stats("replay_click_q"))
        ),
        "bellman_reward": torch.mean(
            torch.stack(policy.get_tower_stats("bellman_reward"))
        ),
        "next_q_values": torch.mean(
            torch.stack(policy.get_tower_stats("next_q_values"))
        ),
        "target": torch.mean(torch.stack(policy.get_tower_stats("target"))),
        "next_q_target_slate": torch.mean(
            torch.stack(policy.get_tower_stats("next_q_target_slate"))
        ),
        "next_q_target_max": torch.mean(
            torch.stack(policy.get_tower_stats("next_q_target_max"))
        ),
        "target_clicked": torch.mean(
            torch.stack(policy.get_tower_stats("target_clicked"))
        ),
        "q_loss": torch.mean(torch.stack(policy.get_tower_stats("q_loss"))),
        "mean_actions": torch.mean(torch.stack(policy.get_tower_stats("mean_actions"))),
        "choice_loss": torch.mean(torch.stack(policy.get_tower_stats("choice_loss"))),
        # "choice_beta": torch.mean(torch.stack(policy.get_tower_stats("choice_beta"))),
        # "choice_score_no_click": torch.mean(
        #    torch.stack(policy.get_tower_stats("choice_score_no_click"))
        # ),
    }
    # model_stats = {
    #    k: torch.mean(var)
    #    for k, var in policy.model.trainable_variables(as_dict=True).items()
    # }
    # stats.update(model_stats)

    return stats


def action_distribution_fn(
    policy: Policy,
    model: SlateQTorchModel,
    input_dict,
    *,
    explore,
    is_training,
    **kwargs,
):
    """Determine which action to take."""

    observation = input_dict[SampleBatch.OBS]

    # user.shape: [B, E]
    user_obs = observation["user"]
    doc_obs = list(observation["doc"].values())

    # Compute scores per candidate.
    scores, score_no_click = score_documents(user_obs, doc_obs)
    # Compute Q-values per candidate.
    q_values = model.get_q_values(user_obs, doc_obs)

    per_slate_q_values = get_per_slate_q_values(
        policy, score_no_click, scores, q_values
    )
    if not hasattr(model, "slates"):
        model.slates = policy.slates
    return per_slate_q_values, TorchCategorical, []


def get_per_slate_q_values(policy, score_no_click, scores, q_values):
    indices = policy.slates_indices.to(scores.device)
    A, S = policy.slates.shape
    slate_q_values = torch.take_along_dim(scores * q_values, indices, dim=1).reshape(
        [-1, A, S]
    )
    slate_scores = torch.take_along_dim(scores, indices, dim=1).reshape([-1, A, S])
    slate_normalizer = torch.sum(slate_scores, dim=2) + score_no_click.unsqueeze(1)

    slate_q_values = slate_q_values / slate_normalizer.unsqueeze(2)
    slate_sum_q_values = torch.sum(slate_q_values, dim=2)
    return slate_sum_q_values


def score_documents(
    user_obs, doc_obs, no_click_score=1.0, multinomial_logits=False, min_normalizer=-1.0
):
    """Computes dot-product scores for user vs doc (plus no-click) feature vectors."""

    # Dot product between used and each document feature vector.
    scores_per_candidate = torch.sum(
        torch.multiply(user_obs.unsqueeze(1), torch.stack(doc_obs, dim=1)), dim=2
    )
    # Compile a constant no-click score tensor.
    score_no_click = torch.full(
        size=[user_obs.shape[0], 1], fill_value=no_click_score
    ).to(scores_per_candidate.device)
    # Concatenate click and no-click scores.
    all_scores = torch.cat([scores_per_candidate, score_no_click], dim=1)

    # Logits: Softmax to yield probabilities.
    if multinomial_logits:
        all_scores = nn.functional.softmax(all_scores)
    # Multinomial proportional model: Shift to `[0.0,..[`.
    else:
        all_scores = all_scores - min_normalizer

    # Return click (per candidate document) and no-click scores.
    return all_scores[:, :-1], all_scores[:, -1]


def setup_early(policy, obs_space, action_space, config):
    """Obtain all possible slates given current docs in the candidate set."""

    num_candidates = action_space.nvec[0]
    slate_size = len(action_space.nvec)

    mesh_args = [torch.Tensor(list(range(num_candidates)))] * slate_size
    slates = torch.stack(torch.meshgrid(*mesh_args), dim=-1)
    slates = torch.reshape(slates, shape=(-1, slate_size))
    # Filter slates that include duplicates to ensure each document is picked
    # at most once.
    unique_mask = []
    for i in range(slates.shape[0]):
        x = slates[i]
        unique_mask.append(len(x) == len(torch.unique(x)))
    unique_mask = torch.Tensor(unique_mask).bool().unsqueeze(1)
    # slates.shape: [A, S]
    slates = torch.masked_select(slates, mask=unique_mask).reshape([-1, slate_size])

    # Store all possible slates only once in policy object.
    policy.slates = slates.long()
    # [1, AxS] Useful for torch.take_along_dim()
    policy.slates_indices = policy.slates.reshape(-1).unsqueeze(0)


def optimizer_fn(
    policy: Policy, config: AlgorithmConfigDict
) -> Tuple["torch.optim.Optimizer"]:
    optimizer_choice = torch.optim.Adam(
        policy.model.choice_model.parameters(), lr=config["lr_choice_model"]
    )
    optimizer_q_value = torch.optim.RMSprop(
        policy.model.q_model.parameters(),
        lr=config["lr"],
        eps=config["rmsprop_epsilon"],
        momentum=0.0,
        weight_decay=0.95,
        centered=True,
    )
    return optimizer_choice, optimizer_q_value


def postprocess_fn_add_next_actions_for_sarsa(
    policy: Policy, batch: SampleBatch, other_agent=None, episode=None
) -> SampleBatch:
    """Add next_actions to SampleBatch for SARSA training"""
    if policy.config["slateq_strategy"] == "SARSA":
        if not batch["dones"][-1] and policy._no_tracing is False:
            raise RuntimeError(
                "Expected a complete episode in each sample batch. "
                f"But this batch is not: {batch}."
            )
        batch["next_actions"] = np.roll(batch["actions"], -1, axis=0)

    return batch


def setup_late_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> None:
    """Call all mixin classes' constructors before SlateQTorchPolicy initialization.

    Args:
        policy: The Policy object.
        obs_space: The Policy's observation space.
        action_space: The Policy's action space.
        config: The Policy's config.
    """
    TargetNetworkMixin.__init__(policy)


SlateQTorchPolicy = build_policy_class(
    name="SlateQTorchPolicy",
    framework="torch",
    get_default_config=lambda: ray.rllib.algorithms.slateq.slateq.DEFAULT_CONFIG,
    before_init=setup_early,
    after_init=setup_late_mixins,
    loss_fn=build_slateq_losses,
    stats_fn=build_slateq_stats,
    # Build model, loss functions, and optimizers
    make_model_and_action_dist=build_slateq_model_and_distribution,
    optimizer_fn=optimizer_fn,
    # Define how to act.
    action_distribution_fn=action_distribution_fn,
    # Post processing sampled trajectory data.
    # postprocess_fn=postprocess_fn_add_next_actions_for_sarsa,
    extra_grad_process_fn=apply_grad_clipping,
    extra_learn_fetches_fn=concat_multi_gpu_td_errors,
    mixins=[TargetNetworkMixin],
)
