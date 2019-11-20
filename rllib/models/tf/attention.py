from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.misc import normc_initializer, get_activation_fn
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


def relative_position_embedding(seq_length, out_dim):
    inverse_freq = 1 / (10000 ** (tf.range(0, out_dim, 2.0) / out_dim))
    pos_offsets = tf.range(seq_length - 1., -1., -1.)
    inputs = pos_offsets[:, None] * inverse_freq[None, :]
    return tf.concat((tf.sin(inputs), tf.cos(inputs)), axis=-1)


def rel_shift(x):
    # shift approach as implemented by Dai et al. 2019
    # taken from: https://github.com/kimiyoung/transformer-xl/blob/44781ed21dbaec88b280f74d9ae2877f52b492a5/tf/model.py#L31
    x_size = tf.shape(x)

    x = tf.pad(x, [[0, 0], [1, 0], [0, 0], [0, 0]])
    x = tf.reshape(x, [x_size[1] + 1, x_size[0], x_size[2], x_size[3]])
    x = tf.slice(x, [1, 0, 0, 0], [-1, -1, -1, -1])
    x = tf.reshape(x, x_size)

    return x


class MultiHeadAttention(tf.keras.layers.Layer):

    def __init__(self, out_dim, num_heads, head_dim):
        # no bias or non-linearity
        self._num_heads = num_heads
        self._head_dim = head_dim
        self._qkv_layer = tf.keras.layers.Dense(3 * num_heads * head_dim,
                                                use_bias=False)
        self._linear_layer = tf.keras.layers.TimeDistributed(
            tf.keras.layers.Dense(out_dim,
                                  use_bias=False))
        self._layer_norm = tf.keras.layers.LayerNormalization(axis=-1)

    def call(self, inputs):
        L = inputs.shape[0]  # length of segment
        H = self._num_heads  # number of attention heads
        D = self._head_dim  # attention head dimension

        qkv = self._qkv_encoder(inputs)

        queries, keys, values = tf.split(qkv, 3, -1)
        queries = queries[-L:]  # only query based on the segment

        queries = tf.reshape(queries, [L, -1, H, D])
        keys = tf.reshape(keys, [L, -1, H, D])
        values = tf.reshape(values, [L, -1, H, D])

        score = tf.einsum("ibhd,jbhd->ijbh", queries, keys)
        score = score / D ** 0.5

        # causal mask of the same length as the sequence
        mask = tf.sequence_mask(tf.range(1, L + 1), dtype=score.dtype)
        mask = mask[:, :, None, None]

        masked_score = score * mask + 1e30 * (mask - 1.)
        wmat = tf.nn.softmax(masked_score, axis=1)

        out = tf.einsum("ijbn,jbnd->ibnd", wmat, values)
        out = inputs + self._linear_layer(out)

        return self._layer_norm(out)


class RelativeMultiHeadAttention(tf.keras.layers.Layer):

    def __init__(self, out_dim, num_heads, head_dim, rel_pos_encoder):
        # no bias or non-linearity
        self._num_heads = num_heads
        self._head_dim = head_dim
        self._qkv_layer = tf.keras.layers.Dense(3 * num_heads * head_dim,
                                                use_bias=False)
        self._linear_layer = tf.keras.layers.TimeDistributed(
            tf.keras.layers.Dense(out_dim,
                                  use_bias=False))
        self._layer_norm = tf.keras.layers.LayerNormalization(axis=-1)

        self._uvar = self.add_weight(shape=(num_heads, head_dim))
        self._vvar = self.add_weight(shape=(num_heads, head_dim))

        self._pos_proj = tf.keras.layers.Dense(num_heads * head_dim,
                                               use_bias=False)
        self._rel_pos_encoder = rel_pos_encoder

    def call(self, inputs, memory=None):
        L = inputs.shape[0]  # length of segment
        H = self._num_heads  # number of attention heads
        D = self._head_dim  # attention head dimension
        M = memory.shape[0]  # length of the memory segment

        if memory is not None:
            inputs = np.concatenate((memory, inputs), axis=-1)

        qkv = self._qkv_encoder(inputs)

        queries, keys, values = tf.split(qkv, 3, -1)
        queries = queries[-L:]  # only query based on the segment

        queries = tf.reshape(queries, [L, -1, H, D])
        keys = tf.reshape(keys, [L + memory.shape[0], -1, H, D])
        values = tf.reshape(values, [L + memory.shape[0], -1, H, D])

        rel = self._pos_proj(self._rel_pos_encoder)
        rel = tf.reshape(rel, [-1, H, D])

        score = tf.einsum("ibhd,jbhd->ijbh", queries + self._uvar, keys)
        pos_score = tf.einsum("ibhd,jhd->ijbh", queries + self._vvar, rel)
        score = score + rel_shift(pos_score)
        score = score / D**0.5

        # causal mask of the same length as the sequence
        mask = tf.sequence_mask(tf.range(M + 1, L + M + 1), dtype=score.dtype)
        mask = mask[:, :, None, None]

        masked_score = score * mask + 1e30 * (mask - 1.)
        wmat = tf.nn.softmax(masked_score, axis=1)

        out = tf.einsum("ijbn,jbnd->ibnd", wmat, values)
        out = inputs + self._linear_layer(out)

        return self._layer_norm(out)


class PositionwiseFeedforward(tf.keras.layers.Layer):

    def __init__(self, out_dim, hidden_dim):
        self._hidden_layer = tf.keras.layers.Dense(
            hidden_dim,
            activation=tf.nn.relu,
        )
        self._output_layer = tf.keras.layers.Dense(out_dim)
        self._layer_norm = tf.keras.layers.LayerNormalization(axis=-1)

    def call(self, inputs, **kwargs):
        output = self._hidden_layer(inputs)
        output = self._output_layer(output)
        return self._layer_norm(output + inputs)


class TransformerXL(TFModelV2):

    def __init__(self):
        pass