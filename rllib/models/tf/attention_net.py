"""
[1] - Attention Is All You Need - Vaswani, Jones, Shazeer, Parmar,
      Uszkoreit, Gomez, Kaiser - Google Brain/Research, U Toronto - 2017.
      https://arxiv.org/pdf/1706.03762.pdf
[2] - Stabilizing Transformers for Reinforcement Learning - E. Parisotto
      et al. - DeepMind - 2019. https://arxiv.org/pdf/1910.06764.pdf
[3] - Transformer-XL: Attentive Language Models Beyond a Fixed-Length Context.
      Z. Dai, Z. Yang, et al. - Carnegie Mellon U - 2019.
      https://www.aclweb.org/anthology/P19-1285.pdf
"""
import numpy as np

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.recurrent_net import RecurrentNetwork
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf

tf = try_import_tf()


# TODO: Move this somewhere else (examples?).
#class MultiHeadAttention(tf.keras.layers.Layer):
#    """A multi-head attention layer described in [1]."""

#    def __init__(self, out_dim, num_heads, head_dim, **kwargs):
#        super().__init__(**kwargs)

#        # No bias or non-linearity.
#        self._num_heads = num_heads
#        self._head_dim = head_dim
#        self._qkv_layer = tf.keras.layers.Dense(
#            3 * num_heads * head_dim, use_bias=False)
#        self._linear_layer = tf.keras.layers.TimeDistributed(
#            tf.keras.layers.Dense(out_dim, use_bias=False))

#    def call(self, inputs):
#        L = tf.shape(inputs)[1]  # length of segment
#        H = self._num_heads  # number of attention heads
#        D = self._head_dim  # attention head dimension

#        qkv = self._qkv_layer(inputs)

#        queries, keys, values = tf.split(qkv, 3, -1)
#        queries = queries[:, -L:]  # only query based on the segment

#        queries = tf.reshape(queries, [-1, L, H, D])
#        keys = tf.reshape(keys, [-1, L, H, D])
#        values = tf.reshape(values, [-1, L, H, D])

#        score = tf.einsum("bihd,bjhd->bijh", queries, keys)
#        score = score / D ** 0.5

#        # causal mask of the same length as the sequence
#        mask = tf.sequence_mask(tf.range(1, L + 1), dtype=score.dtype)
#        mask = mask[None, :, :, None]

#        masked_score = score * mask + 1e30 * (mask - 1.)
#        wmat = tf.nn.softmax(masked_score, axis=2)

#        out = tf.einsum("bijh,bjhd->bihd", wmat, values)
#        out = tf.reshape(out, tf.concat((tf.shape(out)[:2], [H * D]), axis=0))
#        return self._linear_layer(out)


class RelativeMultiHeadAttention(tf.keras.layers.Layer):
    """A RelativeMultiHeadAttention layer as described in [3].

    Uses segment level recurrence with state reuse.
    """
    def __init__(self,
                 out_dim,
                 num_heads,
                 head_dim,
                 rel_pos_encoder,
                 input_layernorm=False,
                 output_activation=None,
                 **kwargs):
        """Initializes a RelativeMultiHeadAttention keras Layer object.

        Args:
            out_dim (int):
            num_heads (int): The number of attention heads to use.
                Denoted `H` in [2].
            head_dim (int): The dimension of a single(!) attention head
                Denoted `D` in [2].
            rel_pos_encoder (:
            input_layernorm (bool): Whether to prepend a LayerNorm before
                everything else. Should be True for building a GTrXL.
            output_activation (Optional[tf.nn.activation]): Optional tf.nn
                activation function. Should be relu for GTrXL.
            **kwargs:
        """
        super().__init__(**kwargs)

        # No bias or non-linearity.
        self._num_heads = num_heads
        self._head_dim = head_dim
        #3=Query, key, and value inputs.
        self._qkv_layer = tf.keras.layers.Dense(
            3 * num_heads * head_dim, use_bias=False)
        self._linear_layer = tf.keras.layers.TimeDistributed(
            tf.keras.layers.Dense(
                out_dim, use_bias=False, activation=output_activation))

        self._uvar = self.add_weight(shape=(num_heads, head_dim))
        self._vvar = self.add_weight(shape=(num_heads, head_dim))

        self._pos_proj = tf.keras.layers.Dense(
            num_heads * head_dim, use_bias=False)
        self._rel_pos_encoder = rel_pos_encoder

        self._input_layernorm = None
        if input_layernorm:
            self._input_layernorm = tf.keras.layers.LayerNormalization(axis=-1)

    def call(self, inputs, memory=None):
        T = tf.shape(inputs)[1]  # length of segment (time)
        H = self._num_heads  # number of attention heads
        d = self._head_dim  # attention head dimension

        # Add previous memory chunk (as const, w/o gradient) to input.
        # Tau (number of (prev) time slices in each memory chunk).
        Tau = memory.shape[0] if memory is not None else 0
        if memory is not None:
            inputs = np.concatenate(
                (tf.stop_gradient(memory), inputs), axis=1)

        # Apply the Layer-Norm.
        if self._input_layernorm is not None:
            inputs = self._input_layernorm(inputs)

        qkv = self._qkv_layer(inputs)

        queries, keys, values = tf.split(qkv, 3, -1)
        queries = queries[:, -T:]  # only query based on the segment

        queries = tf.reshape(queries, [-1, T, H, d])
        keys = tf.reshape(keys, [-1, T + Tau, H, d])
        values = tf.reshape(values, [-1, T + Tau, H, d])

        rel = self._pos_proj(self._rel_pos_encoder)
        rel = tf.reshape(rel, [T, H, d])

        score = tf.einsum("bihd,bjhd->bijh", queries + self._uvar, keys)
        pos_score = tf.einsum("bihd,jhd->bijh", queries + self._vvar, rel)
        score = score + self.rel_shift(pos_score)
        score = score / d**0.5

        # causal mask of the same length as the sequence
        mask = tf.sequence_mask(tf.range(Tau + 1, T + Tau + 1), dtype=score.dtype)
        mask = mask[None, :, :, None]

        masked_score = score * mask + 1e30 * (mask - 1.)
        wmat = tf.nn.softmax(masked_score, axis=2)

        out = tf.einsum("bijh,bjhd->bihd", wmat, values)
        out = tf.reshape(out, tf.concat((tf.shape(out)[:2], [H * d]), axis=0))
        return self._linear_layer(out)

    @staticmethod
    def rel_shift(x):
        # Transposed version of the shift approach described in [3].
        # https://github.com/kimiyoung/transformer-xl/blob/
        # 44781ed21dbaec88b280f74d9ae2877f52b492a5/tf/model.py#L31
        x_size = tf.shape(x)

        x = tf.pad(x, [[0, 0], [0, 0], [1, 0], [0, 0]])
        x = tf.reshape(x, [x_size[0], x_size[2] + 1, x_size[1], x_size[3]])
        x = tf.slice(x, [0, 1, 0, 0], [-1, -1, -1, -1])
        x = tf.reshape(x, x_size)

        return x


class PositionwiseFeedforward(tf.keras.layers.Layer):
    """A 2x linear layer with ReLU activation in between described in [1].

    Each timestep coming from the attention head will be passed through this
    layer separately.
    """

    def __init__(self, out_dim, hidden_dim, output_activation=None, **kwargs):
        super().__init__(**kwargs)

        self._hidden_layer = tf.keras.layers.Dense(
            hidden_dim,
            activation=tf.nn.relu,
        )

        self._output_layer = tf.keras.layers.Dense(
            out_dim, activation=output_activation)

    def call(self, inputs, **kwargs):
        del kwargs
        output = self._hidden_layer(inputs)
        return self._output_layer(output)


class SkipConnection(tf.keras.layers.Layer):
    """Skip connection layer.

    Adds the original input to the output (regular residual layer) OR uses
    input as hidden state input to a given fan_in_layer.
    """

    def __init__(self, layer, fan_in_layer=None, **kwargs):
        """Initializes a SkipConnection keras layer object.

        Args:
            layer (tf.keras.layers.Layer): Any layer processing inputs.
            fan_in_layer (Optional[tf.keras.layers.Layer]): An optional
                layer taking two inputs: The original input and the output
                of `layer`.
        """
        super().__init__(**kwargs)
        self._layer = layer
        self._fan_in_layer = fan_in_layer

    def call(self, inputs, **kwargs):
        del kwargs
        outputs = self._layer(inputs)
        # Residual case, just add inputs to outputs.
        if self._fan_in_layer is None:
            outputs = outputs + inputs
        # Fan-in e.g. RNN: Call fan-in with `inputs` and `outputs`.
        else:
            # NOTE: In the GRU case, `inputs` is the prev. hidden state.
            outputs = self._fan_in_layer((inputs, outputs))

        return outputs


class GRUGate(tf.keras.layers.Layer):

    def __init__(self, init_bias=0., **kwargs):
        super().__init__(**kwargs)
        self._init_bias = init_bias

    def build(self, input_shape):
        h_shape, x_shape = input_shape
        if x_shape[-1] != h_shape[-1]:
            raise ValueError(
                "Both inputs to GRUGate must have equal size in last axis!")

        dim = h_shape[-1]
        self._w_r = self.add_weight(shape=(dim, dim))
        self._w_z = self.add_weight(shape=(dim, dim))
        self._w_h = self.add_weight(shape=(dim, dim))

        self._u_r = self.add_weight(shape=(dim, dim))
        self._u_z = self.add_weight(shape=(dim, dim))
        self._u_h = self.add_weight(shape=(dim, dim))

        def bias_initializer(shape, dtype):
            return tf.fill(shape, tf.cast(self._init_bias, dtype=dtype))

        self._bias_z = self.add_weight(
            shape=(dim, ), initializer=bias_initializer)

    def call(self, inputs, **kwargs):
        # Pass in internal state first.
        h, X = inputs
        r = tf.tensordot(X, self._w_r, axes=1) + \
            tf.tensordot(h, self._u_r, axes=1)
        r = tf.nn.sigmoid(r)

        z = tf.tensordot(X, self._w_z, axes=1) + \
            tf.tensordot(h, self._u_z, axes=1) + self._bias_z
        z = tf.nn.sigmoid(z)

        h_next = tf.tensordot(X, self._w_h, axes=1) + \
                 tf.tensordot((h * r), self._u_h, axes=1)
        h_next = tf.nn.tanh(h_next)

        return (1 - z) * h + z * h_next


#class TrXLNet(TFModelV2):
#    """A TrXL net Model described in [1]."""
#
#    def __init__(self, observation_space, action_space, num_outputs,
#                 model_config, name, seq_length, num_layers, attn_dim,
#                 num_heads, head_dim, ff_hidden_dim):
#        """Initializes a TrXLNet.
#
#        Args:
#            seq_length (int):
#            num_layers (int): The number of repeats to use (N in the paper).
#            attn_dim ():
#            num_heads:
#            head_dim:
#            ff_hidden_dim:
#        """

#        super().__init__(observation_space, action_space, num_outputs,
#                         model_config, name)

#        pos_embedding = relative_position_embedding(seq_length, attn_dim)

#        layers = [tf.keras.layers.Dense(attn_dim)]

#        for _ in range(num_layers):
#            layers.append(
#                SkipConnection(RelativeMultiHeadAttention(
#                    attn_dim, num_heads, head_dim, pos_embedding,
#                    input_layernorm=False,
#                    output_activation=None),
#                    fan_in_layer=None))

#            layers.append(
#                SkipConnection(PositionwiseFeedforward(attn_dim, ff_hidden_dim)))
#            layers.append(tf.keras.layers.LayerNormalization(axis=-1))

#        self.base_model = tf.keras.Sequential(layers)
#        self.register_variables(self.base_model.variables)


class GTrXLNet(RecurrentNetwork):
    """A GTrXL net Model described in [2]."""

    def __init__(self, observation_space, action_space, num_outputs,
                 model_config, name, num_layers, attn_dim,
                 num_heads, head_dim, ff_hidden_dim, init_gate_bias=2.0):
        """Initializes a GTrXLNet.

        Args:
            num_layers (int): The number of repeats to use (denoted L in [2]).
            attn_dim (int): The input dimension of one Transformer unit.
            
            num_heads (int): The number of attention heads to use in parallel.
                Denoted as `H` in [3].
            head_dim (int): The dimension of a single(!) head.
                Denoted as `d` in [3].
            ff_hidden_dim (int):
            init_gate_bias (float):
        """

        super().__init__(observation_space, action_space, num_outputs,
                         model_config, name)

        self.max_seq_len = model_config["max_seq_len"]
        self.obs_dim = observation_space.shape[0]

        # Constant (non-trainable) sinusoid rel pos encoding matrix.
        Phi = self.relative_position_embedding(self.max_seq_len, attn_dim)

        # Raw observation input.
        input_layer = tf.keras.layers.Input(
            shape=(self.max_seq_len, self.obs_dim), name="inputs")

        # Collect the layers for the Transformer.
        # 1) Map observation dim to transformer (attention) dim.
        layers = [tf.keras.layers.Dense(attn_dim)]

        # 2) Create L Transformer blocks according to [2].
        for _ in range(num_layers):
            # RelativeMultiHeadAttention part.
            layers.append(
                SkipConnection(
                    RelativeMultiHeadAttention(
                        attn_dim,
                        num_heads,
                        head_dim,
                        Phi,
                        input_layernorm=True,
                        output_activation=tf.nn.relu),
                    fan_in_layer=GRUGate(init_gate_bias),
                ))
            # Position-wise MLP part.
            layers.append(
                SkipConnection(
                    tf.keras.Sequential(
                        (tf.keras.layers.LayerNormalization(axis=-1),
                         PositionwiseFeedforward(
                             attn_dim, ff_hidden_dim,
                             output_activation=tf.nn.relu))),
                    fan_in_layer=GRUGate(init_gate_bias),
                ))

        self.trxl_core = tf.keras.Sequential(layers)
        trxl_out = self.trxl_core(input_layer)

        #trxl_out = attention.make_GRU_TrXL(
        #    seq_length=model_config["max_seq_len"],
        #    num_layers=model_config["custom_options"]["num_layers"],
        #    attn_dim=model_config["custom_options"]["attn_dim"],
        #    num_heads=model_config["custom_options"]["num_heads"],
        #    head_dim=model_config["custom_options"]["head_dim"],
        #    ff_hidden_dim=model_config["custom_options"]["ff_hidden_dim"],
        #)(input_layer)

        # Postprocess TrXL output with another hidden layer and compute values.
        logits = tf.keras.layers.Dense(
            self.num_outputs,
            activation=tf.keras.activations.linear,
            name="logits")(trxl_out)

        self._value_out = None
        values_out = tf.keras.layers.Dense(
            1, activation=None, name="values")(trxl_out)

        self.trxl_model = tf.keras.Model(
            inputs=[input_layer], outputs=[logits, values_out])

        self.register_variables(self.trxl_model.variables)
        self.trxl_model.summary()

    @override(RecurrentNetwork)
    def forward_rnn(self, inputs, state, seq_lens):
        # To make Attention work with current RLlib's ModelV2 API:
        # We assume `state` is the history of L recent observations (
        # all concatenated into one tensor) and append
        # the current inputs to the end and only keep the most recent (up to
        # `max_seq_len`). This allows us to deal with timestep-wise inference
        # and full sequence training within the same logic.
        state = state[0]
        state = tf.concat((state, inputs), axis=1)[:, -self.max_seq_len:]
        logits, self._value_out = self.trxl_model(state)

        T = tf.shape(inputs)[1]  # Length of input segment (time).
        logits = logits[:, -T:]
        self._value_out = self._value_out[:, -T:]

        return logits, [state]

    @override(RecurrentNetwork)
    def get_initial_state(self):
        # State is the T last observations concat'd together into one Tensor.
        return [np.zeros((self.max_seq_len, self.obs_dim), np.float32)]

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])

    @staticmethod
    def relative_position_embedding(seq_length, out_dim):
        """Creates a [seq_length x seq_length] matrix for rel. pos encoding.

        Denoted as Phi in [2] and [3]. Phi is the standard sinusoid encoding
        matrix.

        Args:
            seq_length (int): The max. sequence length (time axis).
            out_dim (int): The number of nodes to go into the first Tranformer
                layer with.

        Returns:
            tf.Tensor: The encoding matrix Phi.
        """
        inverse_freq = 1 / (10000 ** (tf.range(0, out_dim, 2.0) / out_dim))
        pos_offsets = tf.range(seq_length - 1., -1., -1.)
        inputs = pos_offsets[:, None] * inverse_freq[None, :]
        return tf.concat((tf.sin(inputs), tf.cos(inputs)), axis=-1)
