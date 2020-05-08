"""
[1] - Attention Is All You Need - Vaswani, Jones, Shazeer, Parmar,
      Uszkoreit, Gomez, Kaiser - Google Brain/Research, U Toronto - 2017.
      https://arxiv.org/pdf/1706.03762.pdf
[2] - Stabilizing Transformers for Reinforcement Learning - E. Parisotto
      et. al - DeepMind - 2019. https://arxiv.org/pdf/1910.06764.pdf
"""
import numpy as np

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.recurrent_net import RecurrentNetwork
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_ops import sequence_mask

torch, nn = try_import_torch()


class MultiHeadAttention(nn.Module):
    """A multi-head attention layer described in [1]."""

    def __init__(self, out_dim, num_heads, head_dim, **kwargs):
        super().__init__(**kwargs)

        # No bias or non-linearity.
        self._num_heads = num_heads
        self._head_dim = head_dim
        self._qkv_layer = tf.keras.layers.Dense(
            3 * num_heads * head_dim, use_bias=False)
        self._linear_layer = tf.keras.layers.TimeDistributed(
            tf.keras.layers.Dense(out_dim, use_bias=False))

    def call(self, inputs):
        L = inputs.shape[1]  # length of segment
        H = self._num_heads  # number of attention heads
        D = self._head_dim  # attention head dimension

        qkv = self._qkv_layer(inputs)

        queries, keys, values = tf.split(qkv, 3, -1)
        queries = queries[:, -L:]  # only query based on the segment

        queries = torch.reshape(queries, [-1, L, H, D])
        keys = torch.reshape(keys, [-1, L, H, D])
        values = torch.reshape(values, [-1, L, H, D])

        score = torch.einsum("bihd,bjhd->bijh", queries, keys)
        score = score / D ** 0.5

        # causal mask of the same length as the sequence
        mask = sequence_mask(torch.range(1, L + 1), dtype=score.dtype)
        mask = mask[None, :, :, None]

        masked_score = score * mask + 1e30 * (mask - 1.)
        wmat = nn.functional.softmax(masked_score, axis=2)

        out = torch.einsum("bijh,bjhd->bihd", wmat, values)
        out = torch.reshape(out, torch.cat((out.shape[:2], [H * D]), dim=0))
        return self._linear_layer(out)


class RelativeMultiHeadAttention(nn.Module):
    def __init__(self,
                 out_dim,
                 num_heads,
                 head_dim,
                 rel_pos_encoder,
                 input_layernorm=False,
                 output_activation=None,
                 **kwargs):
        super(RelativeMultiHeadAttention, self).__init__(**kwargs)

        # No bias or non-linearity.
        self._num_heads = num_heads
        self._head_dim = head_dim
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
            self._input_layernorm = tf.keras.layers.LayerNormalization(dim=-1)

    def call(self, inputs, memory=None):
        L = inputs.shape[1]  # length of segment
        H = self._num_heads  # number of attention heads
        D = self._head_dim  # attention head dimension

        # length of the memory segment
        M = memory.shape[0] if memory is not None else 0

        if memory is not None:
            inputs = np.concatenate((memory.detach(), inputs), axis=1)

        if self._input_layernorm is not None:
            inputs = self._input_layernorm(inputs)

        qkv = self._qkv_layer(inputs)

        queries, keys, values = tf.split(qkv, 3, -1)
        queries = queries[:, -L:]  # only query based on the segment

        queries = torch.reshape(queries, [-1, L, H, D])
        keys = torch.reshape(keys, [-1, L + M, H, D])
        values = torch.reshape(values, [-1, L + M, H, D])

        rel = self._pos_proj(self._rel_pos_encoder)
        rel = torch.reshape(rel, [L, H, D])

        score = torch.einsum("bihd,bjhd->bijh", queries + self._uvar, keys)
        pos_score = torch.einsum("bihd,jhd->bijh", queries + self._vvar, rel)
        score = score + self.rel_shift(pos_score)
        score = score / D**0.5

        # causal mask of the same length as the sequence
        mask = sequence_mask(torch.range(M + 1, L + M + 1), dtype=score.dtype)
        mask = mask[None, :, :, None]

        masked_score = score * mask + 1e30 * (mask - 1.)
        wmat = nn.functional.softmax(masked_score, axis=2)

        out = torch.einsum("bijh,bjhd->bihd", wmat, values)
        out = torch.reshape(out, torch.cat((out.shape[:2], [H * D]), dim=0))
        return self._linear_layer(out)

    @staticmethod
    def rel_shift(x):
        # Transposed version of the shift approach implemented by Dai et al. 2019
        # https://github.com/kimiyoung/transformer-xl/blob/
        # 44781ed21dbaec88b280f74d9ae2877f52b492a5/tf/model.py#L31
        x_size = x.shape
    
        x = tf.pad(x, [[0, 0], [0, 0], [1, 0], [0, 0]])
        x = torch.reshape(x, [x_size[0], x_size[2] + 1, x_size[1], x_size[3]])
        x = tf.slice(x, [0, 1, 0, 0], [-1, -1, -1, -1])
        x = torch.reshape(x, x_size)

        return x


class PositionwiseFeedforward(nn.Module):
    """A 2x linear layer with ReLU activation in between described in [1]."""

    def __init__(self, out_dim, hidden_dim, output_activation=None, **kwargs):
        super().__init__(**kwargs)

        self._hidden_layer = SlimFC(
            num_inputs?, hidden_dim,
            activation_fn=nn.ReLU,
        )

        self._output_layer = SlimFC(
            hidden_dim, out_dim, activation_fn=output_activation)

    def call(self, inputs, **kwargs):
        del kwargs
        output = self._hidden_layer(inputs)
        return self._output_layer(output)


class SkipConnection(nn.Module):
    """Skip connection layer.

    If no fan-in layer is specified, then this layer behaves as a regular
    residual layer.
    """

    def __init__(self, layer, fan_in_layer=None, **kwargs):
        super(SkipConnection, self).__init__(**kwargs)
        self._fan_in_layer = fan_in_layer
        self._layer = layer

    def call(self, inputs, **kwargs):
        del kwargs
        outputs = self._layer(inputs)
        if self._fan_in_layer is None:
            outputs = outputs + inputs
        else:
            outputs = self._fan_in_layer((inputs, outputs))

        return outputs


class GRUGate(nn.Module):

    def __init__(self, init_bias=0., **kwargs):
        super(GRUGate, self).__init__(**kwargs)
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

        self._bias_z = self.add_weight(
            shape=(x_shape[-1], ), initializer=bias_initializer)

    def call(self, inputs, **kwargs):
        x, y = inputs
        r = (tf.tensordot(y, self._w_r, axes=1) + tf.tensordot(
            x, self._u_r, axes=1))
        r = tf.nn.sigmoid(r)

        z = (tf.tensordot(y, self._w_z, axes=1) + tf.tensordot(
            x, self._u_z, axes=1) + self._bias_z)
        z = tf.nn.sigmoid(z)

        h = (tf.tensordot(y, self._w_h, axes=1) + tf.tensordot(
            (x * r), self._u_h, axes=1))
        h = tf.nn.tanh(h)

        return (1 - z) * x + z * h


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
#            layers.append(tf.keras.layers.LayerNormalization(dim=-1))

#        self.base_model = tf.keras.Sequential(layers)
#        self.register_variables(self.base_model.variables)


class GTrXLNet(RecurrentNetwork):
    """A GTrXL net Model described in [2]."""

    def __init__(self, observation_space, action_space, num_outputs,
                 model_config, name, num_layers, attn_dim,
                 num_heads, head_dim, ff_hidden_dim, init_gate_bias=2.0):
        """Initializes a GTrXLNet.

        Args:
            num_layers (int): The number of repeats to use (N in the paper).
            attn_dim ():
            num_heads:
            head_dim:
            ff_hidden_dim:
            init_gate_bias (float):
        """

        super().__init__(observation_space, action_space, num_outputs,
                         model_config, name)

        self.max_seq_len = model_config["max_seq_len"]
        self.obs_dim = observation_space.shape[0]
        in_size = self.max_seq_len * self.obs_dim

        pos_embedding = self.relative_position_embedding(
            self.max_seq_len, attn_dim)

        #input_layer = tf.keras.layers.Input(
        #    shape=(self.max_seq_len, self.obs_dim), name="inputs")

        # Collect the layers for the Transformer.
        layers = [SlimFC(in_size, attn_dim, TODO: whats the equivalent for keras.Dense? options)]

        for _ in range(num_layers):
            layers.append(
                SkipConnection(
                    RelativeMultiHeadAttention(
                        attn_dim,
                        num_heads,
                        head_dim,
                        pos_embedding,
                        input_layernorm=True,
                        output_activation=tf.nn.relu),
                    fan_in_layer=GRUGate(init_gate_bias),
                ))

            layers.append(
                SkipConnection(
                    tf.keras.Sequential(
                        (tf.keras.layers.LayerNormalization(dim=-1),
                         PositionwiseFeedforward(
                             in_dim?, attn_dim, ff_hidden_dim,
                             output_activation=tf.nn.relu))),
                    fan_in_layer=GRUGate(init_gate_bias),
                ))

        self.trxl_core = nn.Sequential(layers)
        TODO in forward: trxl_out = self.trxl_core(input_layer)

        #trxl_out = attention.make_GRU_TrXL(
        #    seq_length=model_config["max_seq_len"],
        #    num_layers=model_config["custom_options"]["num_layers"],
        #    attn_dim=model_config["custom_options"]["attn_dim"],
        #    num_heads=model_config["custom_options"]["num_heads"],
        #    head_dim=model_config["custom_options"]["head_dim"],
        #    ff_hidden_dim=model_config["custom_options"]["ff_hidden_dim"],
        #)(input_layer)

        # Postprocess TrXL output with another hidden layer and compute values
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
        state = state[0]

        # We assume state is the history of recent observations and append
        # the current inputs to the end and only keep the most recent (up to
        # max_seq_len). This allows us to deal with timestep-wise inference
        # and full sequence training with the same logic.
        state = torch.cat((state, inputs), dim=1)[:, -self.max_seq_len:]
        logits, self._value_out = self.trxl_model(state)

        in_T = inputs.shape[1]
        logits = logits[:, -in_T:]
        self._value_out = self._value_out[:, -in_T:]

        return logits, [state]

    @override(RecurrentNetwork)
    def get_initial_state(self):
        return [np.zeros((self.max_seq_len, self.obs_dim), np.float32)]

    @override(ModelV2)
    def value_function(self):
        return torch.reshape(self._value_out, [-1])

    @staticmethod
    def relative_position_embedding(seq_length, out_dim):
        inverse_freq = 1 / (10000 ** (torch.range(0, out_dim, 2.0) / out_dim))
        pos_offsets = torch.range(seq_length - 1., -1., -1.)
        inputs = pos_offsets[:, None] * inverse_freq[None, :]
        return torch.cat((torch.sin(inputs), torch.cos(inputs)), dim=-1)
