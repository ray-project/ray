"""Tensorflow model for SlateQ"""

from typing import List

import gym
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import ModelConfigDict, TensorType

tf1, tf, tfv = try_import_tf()


class SlateQTFModel(TFModelV2):
    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        num_outputs: int,
        model_config: ModelConfigDict,
        name: str,
        fcnet_hiddens_per_candidate=(256, 32),
    ):
        """Initializes a SlateQTFModel instance.

        Each document candidate receives one full Q-value stack, defined by
        `fcnet_hiddens_per_candidate`. The input to each of these Q-value stacks
        is always {[user] concat [document[i]] for i in document_candidates}.

        Extra model kwargs:
            fcnet_hiddens_per_candidate: List of layer-sizes for each(!) of the
                candidate documents.
        """
        super(SlateQTFModel, self).__init__(
            obs_space, action_space, None, model_config, name
        )

        self.embedding_size = self.obs_space["doc"]["0"].shape[0]
        self.num_candidates = len(self.obs_space["doc"])
        assert self.obs_space["user"].shape[0] == self.embedding_size

        # Setup the Q head output (i.e., model for get_q_values)
        self.user_in = tf.keras.layers.Input(
            shape=(self.embedding_size,), name="user_in"
        )
        self.docs_in = tf.keras.layers.Input(
            shape=(self.embedding_size * self.num_candidates,), name="docs_in"
        )

        self.num_outputs = num_outputs

        q_outs = []
        for i in range(self.num_candidates):
            doc = self.docs_in[
                :, self.embedding_size * i : self.embedding_size * (i + 1)
            ]
            out = tf.keras.layers.concatenate([self.user_in, doc], axis=1)
            for h in fcnet_hiddens_per_candidate:
                out = tf.keras.layers.Dense(h, activation=tf.nn.relu)(out)
            q_value = tf.keras.layers.Dense(1, name=f"q_value_{i}")(out)
            q_outs.append(q_value)
        q_outs = tf.concat(q_outs, axis=1)

        self.q_value_head = tf.keras.Model([self.user_in, self.docs_in], q_outs)

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
        return self.q_value_head([user, tf.concat(docs, 1)])
