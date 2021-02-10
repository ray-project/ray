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
from gym.spaces import Box, Discrete, MultiDiscrete
import numpy as np
import gym
from typing import Any, Dict, Optional, Union

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.layers import GRUGate, RelativeMultiHeadAttention, \
    SkipConnection
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.recurrent_net import RecurrentNetwork
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import ModelConfigDict, TensorType, List

tf1, tf, tfv = try_import_tf()


# TODO(sven): Use RLlib's FCNet instead.
class PositionwiseFeedforward(tf.keras.layers.Layer if tf else object):
    """A 2x linear layer with ReLU activation in between described in [1].

    Each timestep coming from the attention head will be passed through this
    layer separately.
    """

    def __init__(self,
                 out_dim: int,
                 hidden_dim: int,
                 output_activation: Optional[Any] = None,
                 **kwargs):
        super().__init__(**kwargs)

        self._hidden_layer = tf.keras.layers.Dense(
            hidden_dim,
            activation=tf.nn.relu,
        )

        self._output_layer = tf.keras.layers.Dense(
            out_dim, activation=output_activation)

    def call(self, inputs: TensorType, **kwargs) -> TensorType:
        del kwargs
        output = self._hidden_layer(inputs)
        return self._output_layer(output)


class TrXLNet(RecurrentNetwork):
    """A TrXL net Model described in [1]."""

    def __init__(self, observation_space: gym.spaces.Space,
                 action_space: gym.spaces.Space, num_outputs: int,
                 model_config: ModelConfigDict, name: str,
                 num_transformer_units: int, attention_dim: int,
                 num_heads: int, head_dim: int, position_wise_mlp_dim: int):
        """Initializes a TrXLNet object.

        Args:
            num_transformer_units (int): The number of Transformer repeats to
                use (denoted L in [2]).
            attention_dim (int): The input and output dimensions of one
                Transformer unit.
            num_heads (int): The number of attention heads to use in parallel.
                Denoted as `H` in [3].
            head_dim (int): The dimension of a single(!) attention head within
                a multi-head attention unit. Denoted as `d` in [3].
            position_wise_mlp_dim (int): The dimension of the hidden layer
                within the position-wise MLP (after the multi-head attention
                block within one Transformer unit). This is the size of the
                first of the two layers within the PositionwiseFeedforward. The
                second layer always has size=`attention_dim`.
        """

        super().__init__(observation_space, action_space, num_outputs,
                         model_config, name)

        self.num_transformer_units = num_transformer_units
        self.attention_dim = attention_dim
        self.num_heads = num_heads
        self.head_dim = head_dim
        self.max_seq_len = model_config["max_seq_len"]
        self.obs_dim = observation_space.shape[0]

        inputs = tf.keras.layers.Input(
            shape=(self.max_seq_len, self.obs_dim), name="inputs")
        E_out = tf.keras.layers.Dense(attention_dim)(inputs)

        for _ in range(self.num_transformer_units):
            MHA_out = SkipConnection(
                RelativeMultiHeadAttention(
                    out_dim=attention_dim,
                    num_heads=num_heads,
                    head_dim=head_dim,
                    input_layernorm=False,
                    output_activation=None),
                fan_in_layer=None)(E_out)
            E_out = SkipConnection(
                PositionwiseFeedforward(attention_dim,
                                        position_wise_mlp_dim))(MHA_out)
            E_out = tf.keras.layers.LayerNormalization(axis=-1)(E_out)

        # Postprocess TrXL output with another hidden layer and compute values.
        logits = tf.keras.layers.Dense(
            self.num_outputs,
            activation=tf.keras.activations.linear,
            name="logits")(E_out)

        self.base_model = tf.keras.models.Model([inputs], [logits])

    @override(RecurrentNetwork)
    def forward_rnn(self, inputs: TensorType, state: List[TensorType],
                    seq_lens: TensorType) -> (TensorType, List[TensorType]):
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
    def get_initial_state(self) -> List[np.ndarray]:
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
        >>     attention_dim=32,
        >>     num_heads=2,
        >>     memory_inference=100,
        >>     memory_training=50,
        >>     etc..
        >> }
    """

    def __init__(self,
                 observation_space: gym.spaces.Space,
                 action_space: gym.spaces.Space,
                 num_outputs: Optional[int],
                 model_config: ModelConfigDict,
                 name: str,
                 *,
                 num_transformer_units: int = 1,
                 attention_dim: int = 64,
                 num_heads: int = 2,
                 memory_inference: int = 50,
                 memory_training: int = 50,
                 head_dim: int = 32,
                 position_wise_mlp_dim: int = 32,
                 init_gru_gate_bias: float = 2.0):
        """Initializes a GTrXLNet instance.

        Args:
            num_transformer_units (int): The number of Transformer repeats to
                use (denoted L in [2]).
            attention_dim (int): The input and output dimensions of one
                Transformer unit.
            num_heads (int): The number of attention heads to use in parallel.
                Denoted as `H` in [3].
            memory_inference (int): The number of timesteps to concat (time
                axis) and feed into the next transformer unit as inference
                input. The first transformer unit will receive this number of
                past observations (plus the current one), instead.
            memory_training (int): The number of timesteps to concat (time
                axis) and feed into the next transformer unit as training
                input (plus the actual input sequence of len=max_seq_len).
                The first transformer unit will receive this number of
                past observations (plus the input sequence), instead.
            head_dim (int): The dimension of a single(!) attention head within
                a multi-head attention unit. Denoted as `d` in [3].
            position_wise_mlp_dim (int): The dimension of the hidden layer
                within the position-wise MLP (after the multi-head attention
                block within one Transformer unit). This is the size of the
                first of the two layers within the PositionwiseFeedforward. The
                second layer always has size=`attention_dim`.
            init_gru_gate_bias (float): Initial bias values for the GRU gates
                (two GRUs per Transformer unit, one after the MHA, one after
                the position-wise MLP).
        """

        super().__init__(observation_space, action_space, num_outputs,
                         model_config, name)

        self.num_transformer_units = num_transformer_units
        self.attention_dim = attention_dim
        self.num_heads = num_heads
        self.memory_inference = memory_inference
        self.memory_training = memory_training
        self.head_dim = head_dim
        self.max_seq_len = model_config["max_seq_len"]
        self.obs_dim = observation_space.shape[0]

        # Raw observation input (plus (None) time axis).
        input_layer = tf.keras.layers.Input(
            shape=(None, self.obs_dim), name="inputs")
        memory_ins = [
            tf.keras.layers.Input(
                shape=(None, self.attention_dim),
                dtype=tf.float32,
                name="memory_in_{}".format(i))
            for i in range(self.num_transformer_units)
        ]

        # Map observation dim to input/output transformer (attention) dim.
        E_out = tf.keras.layers.Dense(self.attention_dim)(input_layer)
        # Output, collected and concat'd to build the internal, tau-len
        # Memory units used for additional contextual information.
        memory_outs = [E_out]

        # 2) Create L Transformer blocks according to [2].
        for i in range(self.num_transformer_units):
            # RelativeMultiHeadAttention part.
            MHA_out = SkipConnection(
                RelativeMultiHeadAttention(
                    out_dim=self.attention_dim,
                    num_heads=num_heads,
                    head_dim=head_dim,
                    input_layernorm=True,
                    output_activation=tf.nn.relu),
                fan_in_layer=GRUGate(init_gru_gate_bias),
                name="mha_{}".format(i + 1))(
                    E_out, memory=memory_ins[i])
            # Position-wise MLP part.
            E_out = SkipConnection(
                tf.keras.Sequential(
                    (tf.keras.layers.LayerNormalization(axis=-1),
                     PositionwiseFeedforward(
                         out_dim=self.attention_dim,
                         hidden_dim=position_wise_mlp_dim,
                         output_activation=tf.nn.relu))),
                fan_in_layer=GRUGate(init_gru_gate_bias),
                name="pos_wise_mlp_{}".format(i + 1))(MHA_out)
            # Output of position-wise MLP == E(l-1), which is concat'd
            # to the current Mem block (M(l-1)) to yield E~(l-1), which is then
            # used by the next transformer block.
            memory_outs.append(E_out)

        self._logits = None
        self._value_out = None

        # Postprocess TrXL output with another hidden layer and compute values.
        if num_outputs is not None:
            self._logits = tf.keras.layers.Dense(
                self.num_outputs, activation=None, name="logits")(E_out)
            values_out = tf.keras.layers.Dense(
                1, activation=None, name="values")(E_out)
            outs = [self._logits, values_out]
        else:
            outs = [E_out]
            self.num_outputs = self.attention_dim

        self.trxl_model = tf.keras.Model(
            inputs=[input_layer] + memory_ins, outputs=outs + memory_outs[:-1])

        self.trxl_model.summary()

        # __sphinx_doc_begin__
        # Setup trajectory views (`memory-inference` x past memory outs).
        for i in range(self.num_transformer_units):
            space = Box(-1.0, 1.0, shape=(self.attention_dim, ))
            self.view_requirements["state_in_{}".format(i)] = \
                ViewRequirement(
                    "state_out_{}".format(i),
                    shift="-{}:-1".format(self.memory_inference),
                    # Repeat the incoming state every max-seq-len times.
                    batch_repeat_value=self.max_seq_len,
                    space=space)
            self.view_requirements["state_out_{}".format(i)] = \
                ViewRequirement(
                    space=space,
                    used_for_training=False)
        # __sphinx_doc_end__

    @override(ModelV2)
    def forward(self, input_dict, state: List[TensorType],
                seq_lens: TensorType) -> (TensorType, List[TensorType]):
        assert seq_lens is not None

        # Add the time dim to observations.
        B = tf.shape(seq_lens)[0]
        observations = input_dict[SampleBatch.OBS]

        shape = tf.shape(observations)
        T = shape[0] // B
        observations = tf.reshape(observations,
                                  tf.concat([[-1, T], shape[1:]], axis=0))

        all_out = self.trxl_model([observations] + state)

        if self._logits is not None:
            out = tf.reshape(all_out[0], [-1, self.num_outputs])
            self._value_out = all_out[1]
            memory_outs = all_out[2:]
        else:
            out = tf.reshape(all_out[0], [-1, self.attention_dim])
            memory_outs = all_out[1:]

        return out, [
            tf.reshape(m, [-1, self.attention_dim]) for m in memory_outs
        ]

    # TODO: (sven) Deprecate this once trajectory view API has fully matured.
    @override(RecurrentNetwork)
    def get_initial_state(self) -> List[np.ndarray]:
        return []

    @override(ModelV2)
    def value_function(self) -> TensorType:
        return tf.reshape(self._value_out, [-1])


class AttentionWrapper(TFModelV2):
    """GTrXL wrapper serving as interface for ModelV2s that set use_attention.
    """

    def __init__(self, obs_space: gym.spaces.Space,
                 action_space: gym.spaces.Space, num_outputs: int,
                 model_config: ModelConfigDict, name: str):

        super().__init__(obs_space, action_space, None, model_config, name)

        if isinstance(action_space, Discrete):
            self.action_dim = action_space.n
        elif isinstance(action_space, MultiDiscrete):
            self.action_dim = np.product(action_space.nvec)
        elif action_space.shape is not None:
            self.action_dim = int(np.product(action_space.shape))
        else:
            self.action_dim = int(len(action_space))

        cfg = model_config

        self.attention_dim = cfg["attention_dim"]

        # Construct GTrXL sub-module w/ num_outputs=None (so it does not
        # create a logits/value output; we'll do this ourselves in this wrapper
        # here).
        self.gtrxl = GTrXLNet(
            obs_space,
            action_space,
            None,
            model_config,
            "gtrxl",
            num_transformer_units=cfg["attention_num_transformer_units"],
            attention_dim=self.attention_dim,
            num_heads=cfg["attention_num_heads"],
            head_dim=cfg["attention_head_dim"],
            memory_inference=cfg["attention_memory_inference"],
            memory_training=cfg["attention_memory_training"],
            position_wise_mlp_dim=cfg["attention_position_wise_mlp_dim"],
            init_gru_gate_bias=cfg["attention_init_gru_gate_bias"],
        )

        # `self.num_outputs` right now is the number of nodes coming from the
        # attention net.
        input_ = tf.keras.layers.Input(shape=(self.gtrxl.num_outputs, ))

        # Set final num_outputs to correct value (depending on action space).
        self.num_outputs = num_outputs

        # Postprocess GTrXL output with another hidden layer and compute
        # values.
        out = tf.keras.layers.Dense(self.num_outputs, activation=None)(input_)
        self._logits_branch = tf.keras.models.Model([input_], [out])

        out = tf.keras.layers.Dense(1, activation=None)(input_)
        self._value_branch = tf.keras.models.Model([input_], [out])

        self.view_requirements = self.gtrxl.view_requirements

    @override(RecurrentNetwork)
    def forward(self, input_dict: Dict[str, TensorType],
                state: List[TensorType],
                seq_lens: TensorType) -> (TensorType, List[TensorType]):
        assert seq_lens is not None
        # Push obs through "unwrapped" net's `forward()` first.
        wrapped_out, _ = self._wrapped_forward(input_dict, [], None)

        # Then through our GTrXL.
        input_dict["obs_flat"] = wrapped_out

        self._features, memory_outs = self.gtrxl(input_dict, state, seq_lens)
        model_out = self._logits_branch(self._features)
        return model_out, memory_outs

    @override(ModelV2)
    def get_initial_state(self) -> Union[List[np.ndarray], List[TensorType]]:
        return []

    @override(ModelV2)
    def value_function(self) -> TensorType:
        assert self._features is not None, "Must call forward() first!"
        return tf.reshape(self._value_branch(self._features), [-1])
