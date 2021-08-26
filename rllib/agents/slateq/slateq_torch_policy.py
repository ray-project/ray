"""PyTorch policy class used for SlateQ"""

from typing import Dict, List, Sequence, Tuple

import gym
import numpy as np

import ray
from ray.rllib.models.modelv2 import ModelV2, restore_original_dimensions
from ray.rllib.models.torch.torch_action_dist import (TorchCategorical,
                                                      TorchDistributionWrapper)
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import (ModelConfigDict, TensorType,
                                    TrainerConfigDict)

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional


class QValueModel(nn.Module):
    """The Q-value model for SlateQ"""

    def __init__(self, embedding_size: int, q_hiddens: Sequence[int]):
        super().__init__()

        # construct hidden layers
        layers = []
        ins = 2 * embedding_size
        for n in q_hiddens:
            layers.append(nn.Linear(ins, n))
            layers.append(nn.LeakyReLU())
            ins = n
        layers.append(nn.Linear(ins, 1))
        self.layers = nn.Sequential(*layers)

    def forward(self, user: TensorType, doc: TensorType) -> TensorType:
        """Evaluate the user-doc Q model

        Args:
            user (TensorType): User embedding of shape (batch_size,
                embedding_size).
            doc (TensorType): Doc embeddings of shape (batch_size, num_docs,
                embedding_size).

        Returns:
            score (TensorType): q_values of shape (batch_size, num_docs + 1).
        """
        batch_size, num_docs, embedding_size = doc.shape
        doc_flat = doc.view((batch_size * num_docs, embedding_size))
        user_repeated = user.repeat(num_docs, 1)
        x = torch.cat([user_repeated, doc_flat], dim=1)
        x = self.layers(x)
        # Similar to Google's SlateQ implementation in RecSim, we force the
        # Q-values to zeros if there are no clicks.
        x_no_click = torch.zeros((batch_size, 1), device=x.device)
        return torch.cat([x.view((batch_size, num_docs)), x_no_click], dim=1)


class UserChoiceModel(nn.Module):
    r"""The user choice model for SlateQ

    This class implements a multinomial logit model for predicting user clicks.

    Under this model, the click probability of a document is proportional to:

    .. math::
        \exp(\text{beta} * \text{doc_user_affinity} + \text{score_no_click})
    """

    def __init__(self):
        super().__init__()
        self.beta = nn.Parameter(torch.tensor(0., dtype=torch.float))
        self.score_no_click = nn.Parameter(torch.tensor(0., dtype=torch.float))

    def forward(self, user: TensorType, doc: TensorType) -> TensorType:
        """Evaluate the user choice model

        This function outputs user click scores for candidate documents. The
        exponentials of these scores are proportional user click probabilities.
        Here we return the scores unnormalized because because only some of the
        documents will be selected and shown to the user.

        Args:
            user (TensorType): User embeddings of shape (batch_size,
                embedding_size).
            doc (TensorType): Doc embeddings of shape (batch_size, num_docs,
                embedding_size).

        Returns:
            score (TensorType): logits of shape (batch_size, num_docs + 1),
                where the last dimension represents no_click.
        """
        batch_size = user.shape[0]
        s = torch.einsum("be,bde->bd", user, doc)
        s = s * self.beta
        s = torch.cat([s, self.score_no_click.expand((batch_size, 1))], dim=1)
        return s


class SlateQModel(TorchModelV2, nn.Module):
    """The SlateQ model class

    It includes both the user choice model and the Q-value model.
    """

    def __init__(
            self,
            obs_space: gym.spaces.Space,
            action_space: gym.spaces.Space,
            model_config: ModelConfigDict,
            name: str,
            *,
            embedding_size: int,
            q_hiddens: Sequence[int],
    ):
        nn.Module.__init__(self)
        TorchModelV2.__init__(
            self,
            obs_space,
            action_space,
            # This required parameter (num_outputs) seems redundant: it has no
            # real imact, and can be set arbitrarily. TODO: fix this.
            num_outputs=0,
            model_config=model_config,
            name=name)
        self.choice_model = UserChoiceModel()
        self.q_model = QValueModel(embedding_size, q_hiddens)
        self.slate_size = len(action_space.nvec)

    def choose_slate(self, user: TensorType,
                     doc: TensorType) -> Tuple[TensorType, TensorType]:
        """Build a slate by selecting from candidate documents

        Args:
            user (TensorType): User embeddings of shape (batch_size,
                embedding_size).
            doc (TensorType): Doc embeddings of shape (batch_size,
                num_docs, embedding_size).

        Returns:
            slate_selected (TensorType): Indices of documents selected for
                the slate, with shape (batch_size, slate_size).
            best_slate_q_value (TensorType): The Q-value of the selected slate,
                with shape (batch_size).
        """
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
        best_slate_q_value, max_idx = torch.max(slate_q_values, dim=1)
        # slates_selected.shape: [batch_size, slate_size]
        slates_selected = slates[max_idx]
        return slates_selected, best_slate_q_value

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

        slates_selected, _ = self.choose_slate(user, doc)

        state_out = []
        return slates_selected, state_out


def build_slateq_model_and_distribution(
        policy: Policy, obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: TrainerConfigDict) -> Tuple[ModelV2, TorchDistributionWrapper]:
    """Build models for SlateQ

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
        model_config=config["model"],
        name="slateq_model",
        embedding_size=config["recsim_embedding_size"],
        q_hiddens=config["hiddens"],
    )
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
    # Fields in available in train_batch: ['t', 'eps_id', 'agent_index',
    # 'next_actions', 'obs', 'actions', 'rewards', 'prev_actions',
    # 'prev_rewards', 'dones', 'infos', 'new_obs', 'unroll_id', 'weights',
    # 'batch_indexes']
    learning_strategy = policy.config["slateq_strategy"]

    if learning_strategy == "SARSA":
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
    elif learning_strategy == "MYOP":
        next_q_values = 0.
    elif learning_strategy == "QL":
        # next_doc.shape: [batch_size, num_docs, embedding_size]
        next_doc = torch.cat(
            [val.unsqueeze(1) for val in next_obs["doc"].values()], 1)
        next_user = next_obs["user"]
        dones = train_batch["dones"]
        with torch.no_grad():
            _, next_q_values = model.choose_slate(next_user, next_doc)
        next_q_values[dones] = 0.0
    else:
        raise ValueError(learning_strategy)
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
        policy.model.choice_model.parameters(), lr=config["lr_choice_model"])
    optimizer_q_value = torch.optim.Adam(
        policy.model.q_model.parameters(),
        lr=config["lr_q_model"],
        eps=config["adam_epsilon"])
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

    selected_slates, _ = model.choose_slate(user, doc)

    action = selected_slates
    logp = None
    state_out = []
    return action, logp, state_out


def postprocess_fn_add_next_actions_for_sarsa(policy: Policy,
                                              batch: SampleBatch,
                                              other_agent=None,
                                              episode=None) -> SampleBatch:
    """Add next_actions to SampleBatch for SARSA training"""
    if policy.config["slateq_strategy"] == "SARSA":
        if not batch["dones"][-1]:
            raise RuntimeError(
                "Expected a complete episode in each sample batch. "
                f"But this batch is not: {batch}.")
        batch["next_actions"] = np.roll(batch["actions"], -1, axis=0)
    return batch


SlateQTorchPolicy = build_policy_class(
    name="SlateQTorchPolicy",
    framework="torch",
    get_default_config=lambda: ray.rllib.agents.slateq.slateq.DEFAULT_CONFIG,

    # build model, loss functions, and optimizers
    make_model_and_action_dist=build_slateq_model_and_distribution,
    optimizer_fn=build_slateq_optimizers,
    loss_fn=build_slateq_losses,

    # define how to act
    action_sampler_fn=action_sampler_fn,

    # post processing batch sampled data
    postprocess_fn=postprocess_fn_add_next_actions_for_sarsa,
)
