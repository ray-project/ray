from typing import List, Sequence

import gym
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModelConfigDict, TensorType

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional


class QValueModel(nn.Module):
    def __init__(
        self,
        obs_space: gym.spaces.Space,
        fcnet_hiddens_per_candidate=(256, 32),
    ):
        """Initializes a QValueModel instance.

        Each document candidate receives one full Q-value stack, defined by
        `fcnet_hiddens_per_candidate`. The input to each of these Q-value stacks
        is always {[user] concat [document[i]] for i in document_candidates}.

        Extra model kwargs:
            fcnet_hiddens_per_candidate: List of layer-sizes for each(!) of the
                candidate documents.
        """
        super().__init__()

        self.orig_obs_space = obs_space
        self.embedding_size = self.orig_obs_space["doc"]["0"].shape[0]
        self.num_candidates = len(self.orig_obs_space["doc"])
        assert self.orig_obs_space["user"].shape[0] == self.embedding_size

        self.q_nets = nn.ModuleList()
        for i in range(self.num_candidates):
            layers = nn.Sequential()
            ins = 2 * self.embedding_size
            for j, h in enumerate(fcnet_hiddens_per_candidate):
                layers.add_module(
                    f"q_layer_{i}_{j}",
                    SlimFC(in_size=ins, out_size=h, activation_fn="relu"),
                )
                ins = h
            layers.add_module(f"q_out_{i}", SlimFC(ins, 1, activation_fn=None))

            self.q_nets.append(layers)

    def forward(self, user: TensorType, docs: List[TensorType]) -> TensorType:
        """Returns Q-values, 1 for each candidate document, given user and doc tensors.

        Args:
            user: [B x u] where u=embedding of user features.
            docs: List[[B x d]] where d=embedding of doc features. Each item in the
                list represents one document candidate.

        Returns:
            Tensor ([batch, num candidates) of Q-values.
            1 Q-value per document candidate.
        """
        q_outs = []
        for i in range(self.num_candidates):
            user_cat_doc = torch.cat([user, docs[i]], dim=1)
            q_outs.append(self.q_nets[i](user_cat_doc))

        return torch.cat(q_outs, dim=1)


class UserChoiceModel(nn.Module):
    """The user choice model for SlateQ.

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
        Here we return the scores unnormalized because only some of the
        documents will be selected and shown to the user.

        Args:
            user: User embeddings of shape (batch_size, user embedding size).
            doc: Doc embeddings of shape (batch_size, num_docs, doc embedding size).

        Returns:
            score: logits of shape (batch_size, num_docs + 1),
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


class SlateQTorchModel(TorchModelV2, nn.Module):
    """Initializes a SlateQTFModel instance.

    Model includes both the user choice model and the Q-value model.

    For the Q-value model, each document candidate receives one full Q-value
    stack, defined by `fcnet_hiddens_per_candidate`. The input to each of these
    Q-value stacks is always {[user] concat [document[i]] for i in document_candidates}.

    Extra model kwargs:
        fcnet_hiddens_per_candidate: List of layer-sizes for each(!) of the
            candidate documents.
    """

    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        num_outputs: int,
        model_config: ModelConfigDict,
        name: str,
        *,
        fcnet_hiddens_per_candidate: Sequence[int] = (256, 32),
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
            fcnet_hiddens_per_candidate: List of layer-sizes for each(!) of the
                candidate documents.
            double_q: Whether "double Q-learning" is applied in the loss function.
        """
        nn.Module.__init__(self)
        TorchModelV2.__init__(
            self,
            obs_space,
            action_space,
            # This required parameter (num_outputs) seems redundant: it has no
            # real impact, and can be set arbitrarily. TODO: fix this.
            num_outputs=0,
            model_config=model_config,
            name=name,
        )
        self.num_outputs = num_outputs

        self.choice_model = UserChoiceModel()

        self.q_model = QValueModel(self.obs_space, fcnet_hiddens_per_candidate)

    def get_q_values(self, user: TensorType, docs: List[TensorType]) -> TensorType:
        """Returns Q-values, 1 for each candidate document, given user and doc tensors.

        Args:
            user: [B x u] where u=embedding of user features.
            docs: List[[B x d]] where d=embedding of doc features. Each item in the
                list represents one document candidate.

        Returns:
            Tensor ([batch, num candidates) of Q-values.
            1 Q-value per document candidate.
        """
        return self.q_model(user, docs)
