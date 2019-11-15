from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.misc import normc_initializer, get_activation_fn
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


def positional_embedding():
    pass


class MultiHeadAttention(tf.keras.layers.Layer):

    def __init__(self):
        # no bias or non-linearity
        self._qkv_layer = tf.keras.layers.Dense()
        self._linear_layer = tf.keras.layers.TimeDistributed(
            tf.keras.layers.Dense())
        self._layer_norm = tf.keras.layers.LayerNormalization()

    def call(self, inputs):
        qkv = self._qkv_encoder(inputs)
        queries, keys, values = tf.split(qkv, 3, -1)

        score = tf.einsum("ihtd,ihmd->ihtm", queries, keys)

        # causal mask of the same length as the sequence
        mask = tf.sequence_mask(np.arange(1, score.shape[1] + 1))
        masked_score = score * mask[:, :, None, None] + 1e30*(mask - 1)
        wmat = tf.nn.softmax(masked_score, axis=-1)

        out = tf.einsum("ihtm,ihmd->ihtd", wmat, values)
        out = inputs + self._linear_layer(out)

        return self._layer_norm(out)


class RelativeMultiHeadAttention(tf.keras.layers.Layer):

    def __init__(self, position_encoder):
        # no bias or non-linearity
        self._qkv_layer = tf.keras.layers.Dense()
        self._linear_layer = tf.keras.layers.TimeDistributed(
            tf.keras.layers.Dense())
        self._layer_norm = tf.keras.layers.LayerNormalization()

        self._pos_enc = position_encoder
        self._pos_proj = tf.keras.layers.Dense()

    def call(self, inputs, memory):
        inputs = np.concatenate((memory, inputs), axis=-1)
        qkv = self._qkv_encoder(inputs)
        queries, keys, values = tf.split(qkv, 3, -1)

        rmat = self._pos_proj(self._pos_enc)

        score = None  # WIP


class TransformerXL(TFModelV2):

    def __init__(self):
        pass