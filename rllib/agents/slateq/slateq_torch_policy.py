"""PyTorch policy class used for SlateQ"""

from typing import Dict, List, Tuple

import gym
import numpy as np

import ray
from ray.rllib.models.modelv2 import ModelV2, restore_original_dimensions
from ray.rllib.models.torch.torch_action_dist import (TorchCategorical,
                                                      TorchDistributionWrapper)
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy import LearningRateSchedule
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import (ModelConfigDict, TensorType,
                                    TrainerConfigDict)

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional


class QValueModel(nn.Module):
    def __init__(self, embedding_size: int = 20):
        super().__init__()
        self.layers = nn.Sequential(
            nn.Linear(embedding_size * 2, 256),
            nn.LeakyReLU(),
            nn.Linear(256, 64),
            nn.LeakyReLU(),
            nn.Linear(64, 16),
            nn.LeakyReLU(),
            nn.Linear(16, 1),
        )
        self.no_click_embedding = nn.Parameter(
            torch.randn(embedding_size) / 10.0)

    def forward(self, user: torch.Tensor, doc: torch.Tensor) -> torch.Tensor:
        """Evaluate the user-doc Q model
        Args:
            user (torch.Tensor): User embedding of shape (batch_size,
                embedding_size).
            doc (torch.Tensor): Doc embeddings of shape (batch_size,
                num_docs, embedding_size).
        Returns:
            score (torch.Tensor): q_values of shape (batch_size, num_docs+1).
        """
        batch_size, num_docs, embedding_size = doc.shape
        doc_flat = doc.view((batch_size * num_docs, embedding_size))
        user_repeated = user.repeat(num_docs, 1)
        x = torch.cat([user_repeated, doc_flat], dim=1)
        x = self.layers(x)
        x_no_click = torch.zeros((batch_size, 1), device=x.device)
        # x_no_click = torch.cat(
        #     [user, self.no_click_embedding.repeat(batch_size, 1)], dim=1
        # )
        # x_no_click = self.layers(x_no_click)
        return torch.cat([x.view((batch_size, num_docs)), x_no_click], dim=1)


class UserChoiceModel(nn.Module):
    def __init__(self):
        super().__init__()
        self.a = nn.Parameter(torch.tensor(0, dtype=torch.float))
        self.b = nn.Parameter(torch.tensor(0, dtype=torch.float))

    def forward(self, user: torch.Tensor, doc: torch.Tensor) -> torch.Tensor:
        """Evaluate the user choice model
        Args:
            user (torch.Tensor): User embedding of shape (batch_size,
                embedding_size).
            doc (torch.Tensor): Doc embeddings of shape (batch_size,
                num_docs, embedding_size).
        Returns:
            score (torch.Tensor): logits of shape (batch_size,
                num_docs + 1), where the last dimension represents no_click.
        """
        batch_size = user.shape[0]
        s = torch.einsum("be,bde->bd", user, doc)
        s = s * self.a
        s = torch.cat([s, self.b.expand((batch_size, 1))], dim=1)
        return s


class SlateQModel(TorchModelV2, nn.Module):
    def __init__(
            self,
            obs_space: gym.spaces.Space,
            action_space: gym.spaces.Space,
            num_outputs: int,
            model_config: ModelConfigDict,
            name: str,
    ):
        nn.Module.__init__(self)
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        self.choice_model = UserChoiceModel()
        self.q_model = QValueModel()
        self.slate_size = len(action_space.nvec)

    def choose_slate(self, user: TensorType, doc: TensorType) -> TensorType:
        # Step 1: compute item scores (proportional to click probabilities)
        # raw_scores.shape=[batch_size, num_docs+1]
        raw_scores = self.choice_model(user, doc)
        # max_raw_scores.shape=[batch_size, 1]
        max_raw_scores, _ = torch.max(raw_scores, dim=1, keepdim=True)
        # deduct scores by max_scores to avoid value explosion
        scores = torch.exp(raw_scores - max_raw_scores)
        scores_doc = scores[:, :-1]  # shape=[batch_size, num_docs]
        scores_no_click = scores[:, [-1]]  # shape=[batch_size, 1]

        # Step 2: calculate the item-wise Q values
        # q_values.shape=[batch_size, num_docs+1]
        q_values = self.q_model(user, doc)
        q_values_doc = q_values[:, :-1]  # shape=[batch_size, num_docs]
        q_values_no_click = q_values[:, [-1]]  # shape=[batch_size, 1]

        # Step 3: construct all possible slates
        _, num_docs, _ = doc.shape
        indices = torch.arange(num_docs, dtype=torch.long, device=doc.device)
        # slates.shape = [num_slates, slate_size]
        slates = torch.combinations(indices, r=self.slate_size)
        num_slates, _ = slates.shape

        # Step 4: calculate slate Q values
        batch_size, _ = q_values_doc.shape
        # slate_decomp_q_values.shape: [batch_size, num_slates, slate_size]
        slate_decomp_q_values = torch.gather(
            # input.shape: [batch_size, num_slates, num_docs]
            input=q_values_doc.unsqueeze(1).expand(-1, num_slates, -1),
            dim=2,
            # index.shape: [batch_size, num_slates, slate_size]
            index=slates.unsqueeze(0).expand(batch_size, -1, -1))
        # slate_scores.shape: [batch_size, num_slates, slate_size]
        slate_scores = torch.gather(
            # input.shape: [batch_size, num_slates, num_docs]
            input=scores_doc.unsqueeze(1).expand(-1, num_slates, -1),
            dim=2,
            # index.shape: [batch_size, num_slates, slate_size]
            index=slates.unsqueeze(0).expand(batch_size, -1, -1))
        # slate_q_values.shape: [batch_size, num_slates]
        slate_q_values = ((slate_decomp_q_values * slate_scores).sum(dim=2) +
                          (q_values_no_click * scores_no_click)) / (
                              slate_scores.sum(dim=2) + scores_no_click)

        # Step 5: find the slate that maximizes q value
        max_idx = torch.argmax(slate_q_values, dim=1)
        # slates_selected.shape: [batch_size, slate_size]
        slates_selected = slates[max_idx]
        return slates_selected

    def forward(self, input_dict: Dict[str, TensorType],
                state: List[TensorType],
                seq_lens: TensorType) -> Tuple[TensorType, List[TensorType]]:
        # user.shape: [batch_size, embedding_size]
        user = input_dict[SampleBatch.OBS]["user"]
        # doc.shape: [batch_size, num_docs, embedding_size]
        doc = torch.cat([
            val.unsqueeze(1)
            for val in input_dict[SampleBatch.OBS]["doc"].values()
        ], 1)

        slates_selected = self.choose_slate(user, doc)

        state_out = []
        return slates_selected, state_out


def build_slateq_model_and_distribution(
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
    """
    model = SlateQModel(
        obs_space,
        action_space,
        num_outputs=3,
        model_config=config,
        name="model")
    return model, TorchCategorical


def build_slateq_losses(policy: Policy, model: SlateQModel, _,
                        train_batch: SampleBatch) -> TensorType:
    """Constructs the losses for SlateQPolicy.

    Args:
        policy (Policy): The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        train_batch (SampleBatch): The training data.

    Returns:
        TensorType: A single loss tensor.
    """
    obs = restore_original_dimensions(
        train_batch[SampleBatch.OBS],
        policy.observation_space,
        tensorlib=torch)
    # user.shape: [batch_size, embedding_size]
    user = obs["user"]
    # doc.shape: [batch_size, num_docs, embedding_size]
    doc = torch.cat([val.unsqueeze(1) for val in obs["doc"].values()], 1)
    # action.shape: [batch_size, slate_size]
    actions = train_batch[SampleBatch.ACTIONS]

    next_obs = restore_original_dimensions(
        train_batch[SampleBatch.NEXT_OBS],
        policy.observation_space,
        tensorlib=torch)

    # Step 1: Build user choice model loss
    _, _, embedding_size = doc.shape
    # selected_doc.shape: [batch_size, slate_size, embedding_size]
    selected_doc = torch.gather(
        # input.shape: [batch_size, num_docs, embedding_size]
        input=doc,
        dim=1,
        # index.shape: [batch_size, slate_size, embedding_size]
        index=actions.unsqueeze(2).expand(-1, -1, embedding_size))

    scores = model.choice_model(user, selected_doc)
    choice_loss_fn = nn.CrossEntropyLoss()

    # clicks.shape: [batch_size, slate_size]
    clicks = torch.stack(
        [resp["click"][:, 1] for resp in next_obs["response"]], dim=1)
    no_clicks = 1 - torch.sum(clicks, 1, keepdim=True)
    # clicks.shape: [batch_size, slate_size+1]
    targets = torch.cat([clicks, no_clicks], dim=1)
    choice_loss = choice_loss_fn(scores, torch.argmax(targets, dim=1))
    # print(model.choice_model.a.item(), model.choice_model.b.item())

    # Step 2: Build qvalue loss
    # train_batch: dict_keys(['t', 'eps_id', 'agent_index', 'next_actions',
    # 'obs', 'actions', 'rewards', 'prev_actions', 'prev_rewards',
    # 'dones', 'infos', 'new_obs', 'unroll_id', 'weights', 'batch_indexes'])

    # next_doc.shape: [batch_size, num_docs, embedding_size]
    next_doc = torch.cat(
        [val.unsqueeze(1) for val in next_obs["doc"].values()], 1)
    next_actions = train_batch["next_actions"]
    _, _, embedding_size = next_doc.shape
    # selected_doc.shape: [batch_size, slate_size, embedding_size]
    next_selected_doc = torch.gather(
        # input.shape: [batch_size, num_docs, embedding_size]
        input=next_doc,
        dim=1,
        # index.shape: [batch_size, slate_size, embedding_size]
        index=next_actions.unsqueeze(2).expand(-1, -1, embedding_size))
    next_user = next_obs["user"]
    dones = train_batch["dones"]
    with torch.no_grad():
        # q_values.shape: [batch_size, slate_size+1]
        q_values = model.q_model(next_user, next_selected_doc)
        # raw_scores.shape: [batch_size, slate_size+1]
        raw_scores = model.choice_model(next_user, next_selected_doc)
        max_raw_scores, _ = torch.max(raw_scores, dim=1, keepdim=True)
        scores = torch.exp(raw_scores - max_raw_scores)
        # next_q_values.shape: [batch_size]
        next_q_values = torch.sum(
            q_values * scores, dim=1) / torch.sum(
                scores, dim=1)
        next_q_values[dones] = 0.0
    # target_q_values.shape: [batch_size]
    target_q_values = next_q_values + train_batch["rewards"]

    q_values = model.q_model(user,
                             selected_doc)  # shape: [batch_size, slate_size+1]
    # raw_scores.shape: [batch_size, slate_size+1]
    raw_scores = model.choice_model(user, selected_doc)
    max_raw_scores, _ = torch.max(raw_scores, dim=1, keepdim=True)
    scores = torch.exp(raw_scores - max_raw_scores)
    q_values = torch.sum(
        q_values * scores, dim=1) / torch.sum(
            scores, dim=1)  # shape=[batch_size]

    q_value_loss = nn.MSELoss()(q_values, target_q_values)
    return [choice_loss, q_value_loss]


def build_slateq_optimizers(policy: Policy, config: TrainerConfigDict
                            ) -> List["torch.optim.Optimizer"]:
    optimizer_choice = torch.optim.Adam(
        policy.model.choice_model.parameters(), lr=0.01)
    optimizer_q_value = torch.optim.Adam(
        policy.model.q_model.parameters(), eps=config["adam_epsilon"])
    return [optimizer_choice, optimizer_q_value]


def action_sampler_fn(policy: Policy, model: SlateQModel, input_dict, state,
                      explore, timestep):
    """Determine which action to take"""
    # First, we transform the observation into its unflattened form
    obs = restore_original_dimensions(
        input_dict[SampleBatch.CUR_OBS],
        policy.observation_space,
        tensorlib=torch)

    # user.shape: [batch_size(=1), embedding_size]
    user = obs["user"]
    # doc.shape: [batch_size(=1), num_docs, embedding_size]
    doc = torch.cat([val.unsqueeze(1) for val in obs["doc"].values()], 1)

    selected_slates = model.choose_slate(user, doc)

    action = selected_slates
    # logp = torch.zeros(selected_slates.shape)
    logp = None
    state_out = []
    return action, logp, state_out


def setup_early_mixins(policy: Policy, obs_space, action_space,
                       config: TrainerConfigDict) -> None:
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])


def postprocess_add_next_actions(policy: Policy,
                                 batch: SampleBatch,
                                 other_agent=None,
                                 episode=None) -> SampleBatch:
    assert batch["dones"][-1]
    # Add next actions so that we can use SARSA training
    batch["next_actions"] = np.roll(batch["actions"], -1, axis=0)
    return batch


SlateQTorchPolicy = build_torch_policy(
    name="SlateQTorchPolicy",
    get_default_config=lambda: ray.rllib.agents.slateq.slateq.DEFAULT_CONFIG,

    # build model, loss functions, and optimizers
    make_model_and_action_dist=build_slateq_model_and_distribution,
    optimizer_fn=build_slateq_optimizers,
    loss_fn=build_slateq_losses,

    # define how to act
    action_sampler_fn=action_sampler_fn,

    # post processing batch sampled data
    postprocess_fn=postprocess_add_next_actions,

    # other house keeping items
    before_init=setup_early_mixins,
    mixins=[
        LearningRateSchedule,
    ])
