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
from ray.rllib.models.tf.layers import RelativeMultiHeadAttention, \
    SkipConnection
from ray.rllib.models.tf.recurrent_net import RecurrentNetwork
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf

tf = try_import_tf()


# TODO(sven): Use RLlib's FCNet instead.
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
        memory = state[1]
        logits, self._value_out = self.trxl_model(
            [state, tf.stop_gradient(memory)])

        T = tf.shape(inputs)[1]  # Length of input segment (time).
        logits = logits[:, -T:]
        self._value_out = self._value_out[:, -T:]

        return logits, [state, ]

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
