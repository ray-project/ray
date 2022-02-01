from typing import Dict, List, Optional, Sequence, Tuple

import gym
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModelConfigDict, TensorType

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional


class QValueModel(nn.Module):
    """The Q-value model for SlateQ.

    A simple MLP with n layers (w/ LeakyReLU activation), whose sizes are
    specified via `config.q_hiddens`.
    """

    def __init__(self, embedding_size: int, q_hiddens: Sequence[int]):
        """Initializes a QValueModel instance.

        Args:
            embedding_size: The sum of user- and doc embedding sizes. This is the input
                dimension going into the first layer of the MLP.
            q_hiddens: List of dense layer sizes to build the MLP by.
        """
        super().__init__()

        # Construct hidden layers.
        layers = []
        ins = embedding_size
        for n in q_hiddens:
            layers.append(nn.Linear(ins, n))
            layers.append(nn.LeakyReLU())
            ins = n
        layers.append(nn.Linear(ins, 1))
        self.layers = nn.Sequential(*layers)

    def forward(self, user: TensorType, doc: TensorType) -> TensorType:
        """Evaluate the user-doc Q model

        Args:
            user: User embedding of shape (batch_size, user embedding size).
                Note that `self.embedding_size` is the sum of both user- and
                doc-embedding size.
            doc: Doc embeddings of shape (batch_size, num_docs, doc embedding size).
                Note that `self.embedding_size` is the sum of both user- and
                doc-embedding size.

        Returns:
            The q_values per document of shape (batch_size, num_docs + 1). +1 due to
            also having a Q-value for the non-interaction (no click/no doc).
        """
        batch_size, num_docs, embedding_size = doc.shape
        doc_flat = doc.view((batch_size * num_docs, embedding_size))

        # Concat everything.
        # No user features.
        if user.shape[-1] == 0:
            x = doc_flat
        # User features, repeat user embeddings n times (n=num docs).
        else:
            user_repeated = user.repeat(num_docs, 1)
            x = torch.cat([user_repeated, doc_flat], dim=1)

        x = self.layers(x)

        # Similar to Google's SlateQ implementation in RecSim, we force the
        # Q-values to zeros if there are no clicks.
        # See https://arxiv.org/abs/1905.12767 for details.
        x_no_click = torch.zeros((batch_size, 1), device=x.device)

        return torch.cat([x.view((batch_size, num_docs)), x_no_click], dim=1)


class UserChoiceModel(nn.Module):
    """The user choice model for SlateQ

    This class implements a multinomial logit model for predicting user clicks.

    Under this model, the click probability of a document is proportional to:

    .. math::
        \exp(\text{beta} * \text{doc_user_affinity} + \text{score_no_click})
    """

    def __init__(self):
        """Initializes a UserChoiceModel instance."""
        super().__init__()
        self.beta = nn.Parameter(torch.tensor(0.0, dtype=torch.float))
        self.score_no_click = nn.Parameter(torch.tensor(0.0, dtype=torch.float))

    def forward(self, user: TensorType, doc: TensorType) -> TensorType:
        """Evaluate the user choice model.

        This function outputs user click scores for candidate documents. The
        exponentials of these scores are proportional user click probabilities.
        Here we return the scores unnormalized because because only some of the
        documents will be selected and shown to the user.

        Args:
            user: User embeddings of shape (batch_size, user embedding size).
            doc: Doc embeddings of shape (batch_size, num_docs, doc embedding size).

        Returns:
            score (TensorType): logits of shape (batch_size, num_docs + 1),
                where the last dimension represents no_click.
        """
        batch_size = user.shape[0]
        # Reduce across the embedding axis.
        s = torch.einsum("be,bde->bd", user, doc)
        # s=[batch, num-docs]

        # Multiply with learnable single "click" weight.
        s = s * self.beta
        # Add the learnable no-click score.
        s = torch.cat([s, self.score_no_click.expand((batch_size, 1))], dim=1)

        return s


class SlateQModel(TorchModelV2, nn.Module):
    """The SlateQ model class.

    It includes both the user choice model and the Q-value model.
    """

    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        model_config: ModelConfigDict,
        name: str,
        *,
        user_embedding_size: int,
        doc_embedding_size: int,
        num_docs: int,
        q_hiddens: Sequence[int],
        double_q: bool = True,
    ):
        """Initializes a SlateQModel instance.

        Args:
            user_embedding_size: The size of the user embedding (number of
                user specific features).
            doc_embedding_size: The size of the doc embedding (number of doc
                specific features).
            num_docs: The number of docs to select a slate from. Note that the slate
                size is inferred from the action space.
            q_hiddens: The list of hidden layer sizes for the QValueModel.
            double_q: Whether "double Q-learning" is applied in the loss function.
        """
        nn.Module.__init__(self)
        TorchModelV2.__init__(
            self,
            obs_space,
            action_space,
            # This required parameter (num_outputs) seems redundant: it has no
            # real imact, and can be set arbitrarily. TODO: fix this.
            num_outputs=0,
            model_config=model_config,
            name=name,
        )
        self.choice_model = UserChoiceModel()
        self.q_model = QValueModel(user_embedding_size + doc_embedding_size, q_hiddens)
        self.slate_size = len(action_space.nvec)
        self.double_q = double_q

        self.num_docs = num_docs
        self.indices = torch.arange(
            self.num_docs, dtype=torch.long
        )  # , device=doc.device)
        # slates.shape = [num_slates, slate_size]
        self.slates = torch.combinations(self.indices, r=self.slate_size)
        self.num_slates, _ = self.slates.shape

    def choose_slate(
        self,
        user: TensorType,
        doc: TensorType,
        target_slate_q_values: Optional[TensorType] = None,
    ) -> Tuple[TensorType, TensorType]:
        """Build a slate by selecting from candidate documents

        Args:
            user: User embeddings of shape (batch_size,
                embedding_size).
            doc: Doc embeddings of shape (batch_size, num_docs,
                embedding_size).
            double_q: Whether to apply double-Q correction on the target
                term.

        Returns:
            slate_selected (TensorType): Indices of documents selected for
                the slate, with shape (batch_size, slate_size).
            best_slate_q_value (TensorType): The Q-value of the selected slate,
                with shape (batch_size).
        """
        slate_q_values = self.get_per_slate_q_values(user, doc)

        if target_slate_q_values is not None:
            assert self.double_q
            max_values, best_target_indices = torch.max(target_slate_q_values, dim=1)
            best_slate_q_value = torch.gather(
                slate_q_values, 1, best_target_indices.unsqueeze(1)
            ).squeeze(1)
            # slates_selected.shape: [batch_size, slate_size]
            slates_selected = self.slates[best_target_indices]
        else:
            # Find the slate that maximizes q value.
            best_slate_q_value, max_idx = torch.max(slate_q_values, dim=1)
            # slates_selected.shape: [batch_size, slate_size]
            slates_selected = self.slates[max_idx]

        return slates_selected, best_slate_q_value, slate_q_values

    def get_per_slate_q_values(self, user, doc):
        # Compute item scores (proportional to click probabilities)
        # raw_scores.shape=[batch_size, num_docs+1]
        raw_scores = self.choice_model(user, doc)
        # max_raw_scores.shape=[batch_size, 1]
        max_raw_scores, _ = torch.max(raw_scores, dim=1, keepdim=True)
        # Deduct scores by max_scores to avoid value explosion.
        scores = torch.exp(raw_scores - max_raw_scores)
        scores_doc = scores[:, :-1]  # shape=[batch_size, num_docs]
        scores_no_click = scores[:, [-1]]  # shape=[batch_size, 1]

        # Calculate the item-wise Q values.
        # q_values.shape=[batch_size, num_docs+1]
        q_values = self.q_model(user, doc)
        q_values_doc = q_values[:, :-1]  # shape=[batch_size, num_docs]
        q_values_no_click = q_values[:, [-1]]  # shape=[batch_size, 1]

        # Calculate per-slate Q values.
        batch_size, _ = q_values_doc.shape
        # slate_decomp_q_values.shape: [batch_size, num_slates, slate_size]
        slate_decomp_q_values = torch.gather(
            # input.shape: [batch_size, num_slates, num_docs]
            input=q_values_doc.unsqueeze(1).expand(-1, self.num_slates, -1),
            dim=2,
            # index.shape: [batch_size, num_slates, slate_size]
            index=self.slates.unsqueeze(0).expand(batch_size, -1, -1),
        )
        # slate_scores.shape: [batch_size, num_slates, slate_size]
        slate_scores = torch.gather(
            # input.shape: [batch_size, num_slates, num_docs]
            input=scores_doc.unsqueeze(1).expand(-1, self.num_slates, -1),
            dim=2,
            # index.shape: [batch_size, num_slates, slate_size]
            index=self.slates.unsqueeze(0).expand(batch_size, -1, -1),
        )

        # slate_q_values.shape: [batch_size, num_slates]
        slate_q_values = (
            (slate_decomp_q_values * slate_scores).sum(dim=2)
            + (q_values_no_click * scores_no_click)
        ) / (slate_scores.sum(dim=2) + scores_no_click)
        return slate_q_values

    def forward(
        self,
        input_dict: Dict[str, TensorType],
        state: List[TensorType],
        seq_lens: TensorType,
    ) -> Tuple[TensorType, List[TensorType]]:
        # user.shape: [batch_size, embedding_size]
        user = input_dict[SampleBatch.OBS]["user"]
        # doc.shape: [batch_size, num_docs, embedding_size]
        doc = torch.cat(
            [val.unsqueeze(1) for val in input_dict[SampleBatch.OBS]["doc"].values()], 1
        )

        slates_selected, _, _ = self.choose_slate(user, doc)

        state_out = []
        return slates_selected, state_out
