"""PyTorch policy class used for SlateQ."""

import gym
import logging
import numpy as np
import time
from typing import Dict, List, Tuple, Type

import ray
from ray.rllib.agents.sac.sac_torch_policy import TargetNetworkMixin
from ray.rllib.agents.slateq.slateq_torch_model import SlateQModel
from ray.rllib.models.modelv2 import ModelV2, restore_original_dimensions
from ray.rllib.models.torch.torch_action_dist import (
    TorchCategorical,
    TorchDistributionWrapper,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import apply_grad_clipping, huber_loss
from ray.rllib.utils.typing import TensorType, TrainerConfigDict

torch, nn = try_import_torch()
logger = logging.getLogger(__name__)


def build_slateq_model_and_distribution(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> Tuple[ModelV2, Type[TorchDistributionWrapper]]:
    """Build models for SlateQ

    Args:
        policy: The policy, which will use the model for optimization.
        obs_space: The policy's observation space.
        action_space: The policy's action space.
        config: The Trainer's config dict.

    Returns:
        Tuple consisting of 1) Q-model and 2) an action distribution class.
    """
    model = SlateQModel(
        obs_space,
        action_space,
        model_config=config["model"],
        name="slateq_model",
        user_embedding_size=obs_space.original_space["user"].shape[0],
        doc_embedding_size=obs_space.original_space["doc"]["0"].shape[0],
        num_docs=len(obs_space.original_space["doc"].spaces),
        q_hiddens=config["hiddens"],
        double_q=config["double_q"],
    )

    policy.target_model = SlateQModel(
        obs_space,
        action_space,
        model_config=config["model"],
        name="target_slateq_model",
        user_embedding_size=obs_space.original_space["user"].shape[0],
        doc_embedding_size=obs_space.original_space["doc"]["0"].shape[0],
        num_docs=len(obs_space.original_space["doc"].spaces),
        q_hiddens=config["hiddens"],
        double_q=config["double_q"],
    )

    return model, TorchCategorical


def build_slateq_losses(
    policy: Policy,
    model: ModelV2,
    _: Type[TorchDistributionWrapper],
    train_batch: SampleBatch,
) -> TensorType:
    """Constructs the choice- and Q-value losses for the SlateQTorchPolicy.

    Args:
        policy: The Policy to calculate the loss for.
        model: The Model to calculate the loss for.
        train_batch: The training data.

    Returns:
        Tuple consisting of 1) the choice loss- and 2) the Q-value loss tensors.
    """
    start = time.time()
    obs = restore_original_dimensions(
        train_batch[SampleBatch.OBS], policy.observation_space, tensorlib=torch
    )
    # user.shape: [batch_size, embedding_size]
    user = obs["user"]
    # doc.shape: [batch_size, num_docs, embedding_size]
    doc = torch.cat([val.unsqueeze(1) for val in obs["doc"].values()], 1)
    # action.shape: [batch_size, slate_size]
    actions = train_batch[SampleBatch.ACTIONS]

    next_obs = restore_original_dimensions(
        train_batch[SampleBatch.NEXT_OBS], policy.observation_space, tensorlib=torch
    )

    # Step 1: Build user choice model loss
    _, _, embedding_size = doc.shape
    # selected_doc.shape: [batch_size, slate_size, embedding_size]
    selected_doc = torch.gather(
        # input.shape: [batch_size, num_docs, embedding_size]
        input=doc,
        dim=1,
        # index.shape: [batch_size, slate_size, embedding_size]
        index=actions.unsqueeze(2).expand(-1, -1, embedding_size).long(),
    )

    scores = model.choice_model(user, selected_doc)
    choice_loss_fn = nn.CrossEntropyLoss()

    # clicks.shape: [batch_size, slate_size]
    clicks = torch.stack([resp["click"][:, 1] for resp in next_obs["response"]], dim=1)
    no_clicks = 1 - torch.sum(clicks, 1, keepdim=True)
    # clicks.shape: [batch_size, slate_size+1]
    targets = torch.cat([clicks, no_clicks], dim=1)
    choice_loss = choice_loss_fn(scores, torch.argmax(targets, dim=1))
    # print(model.choice_model.a.item(), model.choice_model.b.item())

    # Step 2: Build qvalue loss
    # Fields in available in train_batch: ['t', 'eps_id', 'agent_index',
    # 'next_actions', 'obs', 'actions', 'rewards', 'prev_actions',
    # 'prev_rewards', 'dones', 'infos', 'new_obs', 'unroll_id', 'weights',
    # 'batch_indexes']
    learning_strategy = policy.config["slateq_strategy"]

    if learning_strategy == "SARSA":
        # next_doc.shape: [batch_size, num_docs, embedding_size]
        next_doc = torch.cat([val.unsqueeze(1) for val in next_obs["doc"].values()], 1)
        next_actions = train_batch["next_actions"]
        _, _, embedding_size = next_doc.shape
        # selected_doc.shape: [batch_size, slate_size, embedding_size]
        next_selected_doc = torch.gather(
            # input.shape: [batch_size, num_docs, embedding_size]
            input=next_doc,
            dim=1,
            # index.shape: [batch_size, slate_size, embedding_size]
            index=next_actions.unsqueeze(2).expand(-1, -1, embedding_size).long(),
        )
        next_user = next_obs["user"]
        dones = train_batch[SampleBatch.DONES]
        with torch.no_grad():
            # q_values.shape: [batch_size, slate_size+1]
            q_values = model.q_model(next_user, next_selected_doc)
            # raw_scores.shape: [batch_size, slate_size+1]
            raw_scores = model.choice_model(next_user, next_selected_doc)
            max_raw_scores, _ = torch.max(raw_scores, dim=1, keepdim=True)
            scores = torch.exp(raw_scores - max_raw_scores)
            # next_q_values.shape: [batch_size]
            next_q_values = torch.sum(q_values * scores, dim=1) / torch.sum(
                scores, dim=1
            )
            next_q_values[dones.bool()] = 0.0
    elif learning_strategy == "MYOP":
        next_q_values = 0.0
    elif learning_strategy == "QL":
        # next_doc.shape: [batch_size, num_docs, embedding_size]
        next_doc = torch.cat([val.unsqueeze(1) for val in next_obs["doc"].values()], 1)
        next_user = next_obs["user"]
        dones = train_batch[SampleBatch.DONES]
        with torch.no_grad():
            if policy.config["double_q"]:
                next_target_per_slate_q_values = policy.target_models[
                    model
                ].get_per_slate_q_values(next_user, next_doc)
                _, next_q_values, _ = model.choose_slate(
                    next_user, next_doc, next_target_per_slate_q_values
                )
            else:
                _, next_q_values, _ = policy.target_models[model].choose_slate(
                    next_user, next_doc
                )
        next_q_values = next_q_values.detach()
        next_q_values[dones.bool()] = 0.0
    else:
        raise ValueError(learning_strategy)
    # target_q_values.shape: [batch_size]
    target_q_values = (
        train_batch[SampleBatch.REWARDS] + policy.config["gamma"] * next_q_values
    )

    # q_values.shape: [batch_size, slate_size+1].
    q_values = model.q_model(user, selected_doc)
    # raw_scores.shape: [batch_size, slate_size+1].
    raw_scores = model.choice_model(user, selected_doc)
    max_raw_scores, _ = torch.max(raw_scores, dim=1, keepdim=True)
    scores = torch.exp(raw_scores - max_raw_scores)
    q_values = torch.sum(q_values * scores, dim=1) / torch.sum(
        scores, dim=1
    )  # shape=[batch_size]
    td_error = torch.abs(q_values - target_q_values)
    q_value_loss = torch.mean(huber_loss(td_error))

    # Store values for stats function in model (tower), such that for
    # multi-GPU, we do not override them during the parallel loss phase.
    model.tower_stats["choice_loss"] = choice_loss
    model.tower_stats["q_loss"] = q_value_loss
    model.tower_stats["q_values"] = q_values
    model.tower_stats["next_q_values"] = next_q_values
    model.tower_stats["next_q_minus_q"] = next_q_values - q_values
    model.tower_stats["td_error"] = td_error
    model.tower_stats["target_q_values"] = target_q_values
    model.tower_stats["raw_scores"] = raw_scores

    logger.debug(f"loss calculation took {time.time()-start}s")
    return choice_loss, q_value_loss


def build_slateq_stats(policy: Policy, batch) -> Dict[str, TensorType]:
    stats = {
        "q_loss": torch.mean(torch.stack(policy.get_tower_stats("q_loss"))),
        "choice_loss": torch.mean(torch.stack(policy.get_tower_stats("choice_loss"))),
        "raw_scores": torch.mean(torch.stack(policy.get_tower_stats("raw_scores"))),
        "q_values": torch.mean(torch.stack(policy.get_tower_stats("q_values"))),
        "next_q_values": torch.mean(
            torch.stack(policy.get_tower_stats("next_q_values"))
        ),
        "next_q_minus_q": torch.mean(
            torch.stack(policy.get_tower_stats("next_q_minus_q"))
        ),
        "target_q_values": torch.mean(
            torch.stack(policy.get_tower_stats("target_q_values"))
        ),
        "td_error": torch.mean(torch.stack(policy.get_tower_stats("td_error"))),
    }
    model_stats = {
        k: torch.mean(var)
        for k, var in policy.model.trainable_variables(as_dict=True).items()
    }
    stats.update(model_stats)
    return stats


def build_slateq_optimizers(
    policy: Policy, config: TrainerConfigDict
) -> List["torch.optim.Optimizer"]:
    optimizer_choice = torch.optim.Adam(
        policy.model.choice_model.parameters(), lr=config["lr_choice_model"]
    )
    optimizer_q_value = torch.optim.Adam(
        policy.model.q_model.parameters(),
        lr=config["lr_q_model"],
        eps=config["adam_epsilon"],
    )
    return [optimizer_choice, optimizer_q_value]


def action_distribution_fn(
    policy: Policy, model: SlateQModel, input_dict, *, explore, is_training, **kwargs
):
    """Determine which action to take"""
    # First, we transform the observation into its unflattened form.

    # start = time.time()
    obs = restore_original_dimensions(
        input_dict[SampleBatch.CUR_OBS], policy.observation_space, tensorlib=torch
    )

    # user.shape: [batch_size(=1), embedding_size]
    user = obs["user"]
    # doc.shape: [batch_size(=1), num_docs, embedding_size]
    doc = torch.cat([val.unsqueeze(1) for val in obs["doc"].values()], 1)

    _, _, per_slate_q_values = model.choose_slate(user, doc)

    return per_slate_q_values, TorchCategorical, []


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
    config: TrainerConfigDict,
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
    get_default_config=lambda: ray.rllib.agents.slateq.slateq.DEFAULT_CONFIG,
    after_init=setup_late_mixins,
    loss_fn=build_slateq_losses,
    stats_fn=build_slateq_stats,
    # Build model, loss functions, and optimizers
    make_model_and_action_dist=build_slateq_model_and_distribution,
    optimizer_fn=build_slateq_optimizers,
    # Define how to act.
    action_distribution_fn=action_distribution_fn,
    # Post processing sampled trajectory data.
    postprocess_fn=postprocess_fn_add_next_actions_for_sarsa,
    extra_grad_process_fn=apply_grad_clipping,
    mixins=[TargetNetworkMixin],
)
