"""
[1] - Attention Is All You Need - Vaswani, Jones, Shazeer, Parmar,
      Uszkoreit, Gomez, Kaiser - Google Brain/Research, U Toronto - 2017.
      https://arxiv.org/pdf/1706.03762.pdf
"""
from ray._common.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorType
from ray.util import log_once

tf1, tf, tfv = try_import_tf()


class MultiHeadAttention(tf.keras.layers.Layer if tf else object):
    """A multi-head attention layer described in [1]."""

    def __init__(self, out_dim: int, num_heads: int, head_dim: int, **kwargs):
        super().__init__(**kwargs)

        # No bias or non-linearity.
        self._num_heads = num_heads
        self._head_dim = head_dim
        self._qkv_layer = tf.keras.layers.Dense(
            3 * num_heads * head_dim, use_bias=False
        )
        self._linear_layer = tf.keras.layers.TimeDistributed(
            tf.keras.layers.Dense(out_dim, use_bias=False)
        )
        if log_once("multi_head_attention"):
            deprecation_warning(
                old="rllib.models.tf.layers.MultiHeadAttention",
            )

    def call(self, inputs: TensorType) -> TensorType:
        L = tf.shape(inputs)[1]  # length of segment
        H = self._num_heads  # number of attention heads
        D = self._head_dim  # attention head dimension

        qkv = self._qkv_layer(inputs)

        queries, keys, values = tf.split(qkv, 3, -1)
        queries = queries[:, -L:]  # only query based on the segment

        queries = tf.reshape(queries, [-1, L, H, D])
        keys = tf.reshape(keys, [-1, L, H, D])
        values = tf.reshape(values, [-1, L, H, D])

        score = tf.einsum("bihd,bjhd->bijh", queries, keys)
        score = score / D**0.5

        # causal mask of the same length as the sequence
        mask = tf.sequence_mask(tf.range(1, L + 1), dtype=score.dtype)
        mask = mask[None, :, :, None]

        masked_score = score * mask + 1e30 * (mask - 1.0)
        wmat = tf.nn.softmax(masked_score, axis=2)

        out = tf.einsum("bijh,bjhd->bihd", wmat, values)
        shape = tf.concat([tf.shape(out)[:2], [H * D]], axis=0)
        out = tf.reshape(out, shape)
        return self._linear_layer(out)
