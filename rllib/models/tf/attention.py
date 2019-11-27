from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.rllib.models.tf.tf_modelv2 import TFModelV2
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

    def __init__(self, out_dim, num_heads, head_dim, **kwargs):
        super(MultiHeadAttention, self).__init__(**kwargs)

        # no bias or non-linearity
        self._num_heads = num_heads
        self._head_dim = head_dim
        self._qkv_layer = tf.keras.layers.Dense(3 * num_heads * head_dim,
                                                use_bias=False)
        self._linear_layer = tf.keras.layers.TimeDistributed(
            tf.keras.layers.Dense(out_dim,
                                  use_bias=False))

    def call(self, inputs):
        L = tf.shape(inputs)[0]  # length of segment
        H = self._num_heads  # number of attention heads
        D = self._head_dim  # attention head dimension

        qkv = self._qkv_layer(inputs)

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
        return self._linear_layer(out)


class RelativeMultiHeadAttention(tf.keras.layers.Layer):

    def __init__(self, out_dim, num_heads, head_dim, rel_pos_encoder, **kwargs):
        super(RelativeMultiHeadAttention, self).__init__(**kwargs)

        # no bias or non-linearity
        self._num_heads = num_heads
        self._head_dim = head_dim
        self._qkv_layer = tf.keras.layers.Dense(3 * num_heads * head_dim,
                                                use_bias=False)
        self._linear_layer = tf.keras.layers.TimeDistributed(
            tf.keras.layers.Dense(out_dim,
                                  use_bias=False))

        self._uvar = self.add_weight(shape=(num_heads, head_dim))
        self._vvar = self.add_weight(shape=(num_heads, head_dim))

        self._pos_proj = tf.keras.layers.Dense(num_heads * head_dim,
                                               use_bias=False)
        self._rel_pos_encoder = rel_pos_encoder

    def call(self, inputs, memory=None):
        L = tf.shape(inputs)[0]  # length of segment
        H = self._num_heads  # number of attention heads
        D = self._head_dim  # attention head dimension

        # length of the memory segment
        M = memory.shape[0] if memory is not None else 0

        if memory is not None:
            inputs = np.concatenate((memory, inputs), axis=-1)

        qkv = self._qkv_layer(inputs)

        queries, keys, values = tf.split(qkv, 3, -1)
        queries = queries[-L:]  # only query based on the segment

        queries = tf.reshape(queries, [L, -1, H, D])
        keys = tf.reshape(keys, [L + M, -1, H, D])
        values = tf.reshape(values, [L + M, -1, H, D])

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
        out = tf.reshape(out, [out.shape[0], out.shape[1], H * D])
        return self._linear_layer(out)


class PositionwiseFeedforward(tf.keras.layers.Layer):

    def __init__(self, out_dim, hidden_dim, **kwargs):
        super(PositionwiseFeedforward, self).__init__(**kwargs)

        self._hidden_layer = tf.keras.layers.Dense(
            hidden_dim,
            activation=tf.nn.relu,
        )
        self._output_layer = tf.keras.layers.Dense(out_dim)
        self._layer_norm = tf.keras.layers.LayerNormalization(axis=-1)

    def call(self, inputs, **kwargs):
        output = self._hidden_layer(inputs)
        return self._output_layer(output)


class SkipConnection(tf.keras.layers.Layer):
    """Skip connection layer.

    If no fan-in layer is specified, then this layer behaves as a regular
    residual layer.
    """

    def __init__(self, layer, fan_in_layer=None, **kwargs):
        super(SkipConnection, self).__init__(**kwargs)
        self._fan_in_layer = fan_in_layer
        self._layer = layer

    def call(self, inputs, **kwargs):
        outputs = self._layer(inputs)
        if self._fan_in_layer is None:
            outputs = outputs + inputs
        else:
            outputs = self._fan_in_layer((inputs, outputs))

        return outputs


class GRUGate(tf.keras.layers.Layer):

    def __init__(self, init_bias=0.):
        self._init_bias = init_bias

    def build(self, input_shape):
        x_shape, y_shape = input_shape
        if x_shape[-1] != y_shape[-1]:
            raise ValueError(
                "Both inputs to GRUGate must equal size last axis.")

        self._w_r = self.add_weight(shape=(y_shape[-1], y_shape[-1]))
        self._w_z = self.add_weight(shape=(y_shape[-1], y_shape[-1]))
        self._w_h = self.add_weight(shape=(y_shape[-1], y_shape[-1]))
        self._u_r = self.add_weight(shape=(x_shape[-1], x_shape[-1]))
        self._u_z = self.add_weight(shape=(x_shape[-1], x_shape[-1]))
        self._u_h = self.add_weight(shape=(x_shape[-1], x_shape[-1]))

        def bias_initializer(shape, dtype):
            return tf.fill(shape, tf.cast(self._init_bias, dtype=dtype))

        self._bias_z = self.add_weight(shape=(x_shape[-1],),
                                       initializer=bias_initializer)

    def call(self, inputs, **kwargs):
        x, y = inputs
        r = (tf.tensordot(self._w_r, y, axes=1)
             + tf.tensordot(self._u_r, x, axes=1))
        r = tf.nn.sigmoid(r)

        z = (tf.tensordot(self._w_z, y, axes=1)
             + tf.tensordot(self._u_z, x, axes=1)
             + self._bias_z)
        z = tf.nn.sigmoid(z)

        h = (tf.tensordot(self._w_h, y, axes=1)
             + tf.tensordot(self._u_h, (x * r), axes=1))
        h = tf.nn.tanh(h)

        return (1 - z) * x + z * h


class TransformerXL(TFModelV2):

    def __init__(self):
        pass