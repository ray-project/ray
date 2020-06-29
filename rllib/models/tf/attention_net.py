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
from ray.rllib.models.tf.layers import GRUGate, RelativeMultiHeadAttention, \
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


class TrXLNet(RecurrentNetwork):
    """A TrXL net Model described in [1]."""

    def __init__(self, observation_space, action_space, num_outputs,
                 model_config, name, num_transformer_units, attn_dim,
                 num_heads, head_dim, ff_hidden_dim):
        """Initializes a TfXLNet object.

        Args:
            num_transformer_units (int): The number of Transformer repeats to
                use (denoted L in [2]).
            attn_dim (int): The input and output dimensions of one Transformer
                unit.
            num_heads (int): The number of attention heads to use in parallel.
                Denoted as `H` in [3].
            head_dim (int): The dimension of a single(!) head.
                Denoted as `d` in [3].
            ff_hidden_dim (int): The dimension of the hidden layer within
                the position-wise MLP (after the multi-head attention block
                within one Transformer unit). This is the size of the first
                of the two layers within the PositionwiseFeedforward. The
                second layer always has size=`attn_dim`.
        """

        super().__init__(observation_space, action_space, num_outputs,
                         model_config, name)

        self.num_transformer_units = num_transformer_units
        self.attn_dim = attn_dim
        self.num_heads = num_heads
        self.head_dim = head_dim
        self.max_seq_len = model_config["max_seq_len"]
        self.obs_dim = observation_space.shape[0]

        pos_embedding = relative_position_embedding(self.max_seq_len, attn_dim)

        inputs = tf.keras.layers.Input(
            shape=(self.max_seq_len, self.obs_dim), name="inputs")
        E_out = tf.keras.layers.Dense(attn_dim)(inputs)

        for _ in range(self.num_transformer_units):
            MHA_out = SkipConnection(
                RelativeMultiHeadAttention(
                    out_dim=attn_dim,
                    num_heads=num_heads,
                    head_dim=head_dim,
                    rel_pos_encoder=pos_embedding,
                    input_layernorm=False,
                    output_activation=None),
                fan_in_layer=None)(E_out)
            E_out = SkipConnection(
                PositionwiseFeedforward(attn_dim, ff_hidden_dim))(MHA_out)
            E_out = tf.keras.layers.LayerNormalization(axis=-1)(E_out)

        # Postprocess TrXL output with another hidden layer and compute values.
        logits = tf.keras.layers.Dense(
            self.num_outputs,
            activation=tf.keras.activations.linear,
            name="logits")(E_out)

        self.base_model = tf.keras.models.Model([inputs], [logits])
        self.register_variables(self.base_model.variables)

    @override(RecurrentNetwork)
    def forward_rnn(self, inputs, state, seq_lens):
        # To make Attention work with current RLlib's ModelV2 API:
        # We assume `state` is the history of L recent observations (all
        # concatenated into one tensor) and append the current inputs to the
        # end and only keep the most recent (up to `max_seq_len`). This allows
        # us to deal with timestep-wise inference and full sequence training
        # within the same logic.
        observations = state[0]
        observations = tf.concat(
            (observations, inputs), axis=1)[:, -self.max_seq_len:]
        logits = self.base_model([observations])
        T = tf.shape(inputs)[1]  # Length of input segment (time).
        logits = logits[:, -T:]

        return logits, [observations]

    @override(RecurrentNetwork)
    def get_initial_state(self):
        # State is the T last observations concat'd together into one Tensor.
        # Plus all Transformer blocks' E(l) outputs concat'd together (up to
        # tau timesteps).
        return [np.zeros((self.max_seq_len, self.obs_dim), np.float32)]


class GTrXLNet(RecurrentNetwork):
    """A GTrXL net Model described in [2].

    This is still in an experimental phase.
    Can be used as a drop-in replacement for LSTMs in PPO and IMPALA.
    For an example script, see: `ray/rllib/examples/attention_net.py`.

    To use this network as a replacement for an RNN, configure your Trainer
    as follows:

    Examples:
        >> config["model"]["custom_model"] = GTrXLNet
        >> config["model"]["max_seq_len"] = 10
        >> config["model"]["custom_model_config"] = {
        >>     num_transformer_units=1,
        >>     attn_dim=32,
        >>     num_heads=2,
        >>     memory_tau=50,
        >>     etc..
        >> }
    """

    def __init__(self,
                 observation_space,
                 action_space,
                 num_outputs,
                 model_config,
                 name,
                 num_transformer_units,
                 attn_dim,
                 num_heads,
                 memory_tau,
                 head_dim,
                 ff_hidden_dim,
                 init_gate_bias=2.0):
        """Initializes a GTrXLNet.

        Args:
            num_transformer_units (int): The number of Transformer repeats to
                use (denoted L in [2]).
            attn_dim (int): The input and output dimensions of one Transformer
                unit.
            num_heads (int): The number of attention heads to use in parallel.
                Denoted as `H` in [3].
            memory_tau (int): The number of timesteps to store in each
                transformer block's memory M (concat'd over time and fed into
                next transformer block as input).
            head_dim (int): The dimension of a single(!) head.
                Denoted as `d` in [3].
            ff_hidden_dim (int): The dimension of the hidden layer within
                the position-wise MLP (after the multi-head attention block
                within one Transformer unit). This is the size of the first
                of the two layers within the PositionwiseFeedforward. The
                second layer always has size=`attn_dim`.
            init_gate_bias (float): Initial bias values for the GRU gates (two
                GRUs per Transformer unit, one after the MHA, one after the
                position-wise MLP).
        """

        super().__init__(observation_space, action_space, num_outputs,
                         model_config, name)

        self.num_transformer_units = num_transformer_units
        self.attn_dim = attn_dim
        self.num_heads = num_heads
        self.memory_tau = memory_tau
        self.head_dim = head_dim
        self.max_seq_len = model_config["max_seq_len"]
        self.obs_dim = observation_space.shape[0]

        # Constant (non-trainable) sinusoid rel pos encoding matrix.
        Phi = relative_position_embedding(self.max_seq_len + self.memory_tau,
                                          self.attn_dim)

        # Raw observation input.
        input_layer = tf.keras.layers.Input(
            shape=(self.max_seq_len, self.obs_dim), name="inputs")
        memory_ins = [
            tf.keras.layers.Input(
                shape=(self.memory_tau, self.attn_dim),
                dtype=tf.float32,
                name="memory_in_{}".format(i))
            for i in range(self.num_transformer_units)
        ]

        # Map observation dim to input/output transformer (attention) dim.
        E_out = tf.keras.layers.Dense(self.attn_dim)(input_layer)
        # Output, collected and concat'd to build the internal, tau-len
        # Memory units used for additional contextual information.
        memory_outs = [E_out]

        # 2) Create L Transformer blocks according to [2].
        for i in range(self.num_transformer_units):
            # RelativeMultiHeadAttention part.
            MHA_out = SkipConnection(
                RelativeMultiHeadAttention(
                    out_dim=self.attn_dim,
                    num_heads=num_heads,
                    head_dim=head_dim,
                    rel_pos_encoder=Phi,
                    input_layernorm=True,
                    output_activation=tf.nn.relu),
                fan_in_layer=GRUGate(init_gate_bias),
                name="mha_{}".format(i + 1))(
                    E_out, memory=memory_ins[i])
            # Position-wise MLP part.
            E_out = SkipConnection(
                tf.keras.Sequential(
                    (tf.keras.layers.LayerNormalization(axis=-1),
                     PositionwiseFeedforward(
                         out_dim=self.attn_dim,
                         hidden_dim=ff_hidden_dim,
                         output_activation=tf.nn.relu))),
                fan_in_layer=GRUGate(init_gate_bias),
                name="pos_wise_mlp_{}".format(i + 1))(MHA_out)
            # Output of position-wise MLP == E(l-1), which is concat'd
            # to the current Mem block (M(l-1)) to yield E~(l-1), which is then
            # used by the next transformer block.
            memory_outs.append(E_out)

        # Postprocess TrXL output with another hidden layer and compute values.
        logits = tf.keras.layers.Dense(
            self.num_outputs,
            activation=tf.keras.activations.linear,
            name="logits")(E_out)

        self._value_out = None
        values_out = tf.keras.layers.Dense(
            1, activation=None, name="values")(E_out)

        self.trxl_model = tf.keras.Model(
            inputs=[input_layer] + memory_ins,
            outputs=[logits, values_out] + memory_outs[:-1])

        self.register_variables(self.trxl_model.variables)
        self.trxl_model.summary()

    @override(RecurrentNetwork)
    def forward_rnn(self, inputs, state, seq_lens):
        # To make Attention work with current RLlib's ModelV2 API:
        # We assume `state` is the history of L recent observations (all
        # concatenated into one tensor) and append the current inputs to the
        # end and only keep the most recent (up to `max_seq_len`). This allows
        # us to deal with timestep-wise inference and full sequence training
        # within the same logic.
        observations = state[0]
        memory = state[1:]

        observations = tf.concat(
            (observations, inputs), axis=1)[:, -self.max_seq_len:]
        all_out = self.trxl_model([observations] + memory)
        logits, self._value_out = all_out[0], all_out[1]
        memory_outs = all_out[2:]
        # If memory_tau > max_seq_len -> overlap w/ previous `memory` input.
        if self.memory_tau > self.max_seq_len:
            memory_outs = [
                tf.concat(
                    [memory[i][:, -(self.memory_tau - self.max_seq_len):], m],
                    axis=1) for i, m in enumerate(memory_outs)
            ]
        else:
            memory_outs = [m[:, -self.memory_tau:] for m in memory_outs]

        T = tf.shape(inputs)[1]  # Length of input segment (time).
        logits = logits[:, -T:]
        self._value_out = self._value_out[:, -T:]

        return logits, [observations] + memory_outs

    @override(RecurrentNetwork)
    def get_initial_state(self):
        # State is the T last observations concat'd together into one Tensor.
        # Plus all Transformer blocks' E(l) outputs concat'd together (up to
        # tau timesteps).
        return [np.zeros((self.max_seq_len, self.obs_dim), np.float32)] + \
               [np.zeros((self.memory_tau, self.attn_dim), np.float32)
                for _ in range(self.num_transformer_units)]

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])


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
    inverse_freq = 1 / (10000**(tf.range(0, out_dim, 2.0) / out_dim))
    pos_offsets = tf.range(seq_length - 1., -1., -1.)
    inputs = pos_offsets[:, None] * inverse_freq[None, :]
    return tf.concat((tf.sin(inputs), tf.cos(inputs)), axis=-1)
