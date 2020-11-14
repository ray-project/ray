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
import gym

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.modules import GRUGate, \
    RelativeMultiHeadAttention, SkipConnection
from ray.rllib.models.torch.recurrent_net import RecurrentNetwork
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModelConfigDict, TensorType, List

torch, nn = try_import_torch()


def relative_position_embedding(seq_length: int, out_dim: int) -> TensorType:
    """Creates a [seq_length x seq_length] matrix for rel. pos encoding.

    Denoted as Phi in [2] and [3]. Phi is the standard sinusoid encoding
    matrix.

    Args:
        seq_length (int): The max. sequence length (time axis).
        out_dim (int): The number of nodes to go into the first Tranformer
            layer with.

    Returns:
        torch.Tensor: The encoding matrix Phi.
    """
    inverse_freq = 1 / (10000**(torch.arange(0, out_dim, 2.0) / out_dim))
    pos_offsets = torch.arange(seq_length - 1, -1, -1)
    inputs = pos_offsets[:, None] * inverse_freq[None, :]
    return torch.cat((torch.sin(inputs), torch.cos(inputs)), dim=-1)


class GTrXLNet(RecurrentNetwork, nn.Module):
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
                 observation_space: gym.spaces.Space,
                 action_space: gym.spaces.Space,
                 num_outputs: int,
                 model_config: ModelConfigDict,
                 name: str,
                 num_transformer_units: int,
                 attn_dim: int,
                 num_heads: int,
                 memory_tau: int,
                 head_dim: int,
                 ff_hidden_dim: int,
                 init_gate_bias: float = 2.0):
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

        nn.Module.__init__(self)

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

        self.linear_layer = SlimFC(
            in_size=self.obs_dim, out_size=self.attn_dim)

        self.layers = [self.linear_layer]

        # 2) Create L Transformer blocks according to [2].
        for i in range(self.num_transformer_units):
            # RelativeMultiHeadAttention part.
            MHA_layer = SkipConnection(
                RelativeMultiHeadAttention(
                    in_dim=self.attn_dim,
                    out_dim=self.attn_dim,
                    num_heads=num_heads,
                    head_dim=head_dim,
                    rel_pos_encoder=Phi,
                    input_layernorm=True,
                    output_activation=nn.ReLU),
                fan_in_layer=GRUGate(self.attn_dim, init_gate_bias))

            # Position-wise MultiLayerPerceptron part.
            E_layer = SkipConnection(
                nn.Sequential(
                    torch.nn.LayerNorm(self.attn_dim),
                    SlimFC(
                        in_size=self.attn_dim,
                        out_size=ff_hidden_dim,
                        use_bias=False,
                        activation_fn=nn.ReLU),
                    SlimFC(
                        in_size=ff_hidden_dim,
                        out_size=self.attn_dim,
                        use_bias=False,
                        activation_fn=nn.ReLU)),
                fan_in_layer=GRUGate(self.attn_dim, init_gate_bias))

            # Build a list of all layers in order.
            self.layers.extend([MHA_layer, E_layer])

        # Postprocess GTrXL output with another hidden layer.
        self.logits = SlimFC(
            in_size=self.attn_dim,
            out_size=self.num_outputs,
            activation_fn=nn.ReLU)

        # Value function used by all RLlib Torch RL implementations.
        self._value_out = None
        self.values_out = SlimFC(
            in_size=self.attn_dim, out_size=1, activation_fn=None)

    @override(RecurrentNetwork)
    def forward_rnn(self, inputs: TensorType, state: List[TensorType],
                    seq_lens: TensorType) -> (TensorType, List[TensorType]):
        # To make Attention work with current RLlib's ModelV2 API:
        # We assume `state` is the history of L recent observations (all
        # concatenated into one tensor) and append the current inputs to the
        # end and only keep the most recent (up to `max_seq_len`). This allows
        # us to deal with timestep-wise inference and full sequence training
        # within the same logic.
        state = [torch.from_numpy(item) for item in state]
        observations = state[0]
        memory = state[1:]

        inputs = torch.reshape(inputs, [1, -1, observations.shape[-1]])
        observations = torch.cat(
            (observations, inputs), axis=1)[:, -self.max_seq_len:]

        all_out = observations
        for i in range(len(self.layers)):
            # MHA layers which need memory passed in.
            if i % 2 == 1:
                all_out = self.layers[i](all_out, memory=memory[i // 2])
            # Either linear layers or MultiLayerPerceptrons.
            else:
                all_out = self.layers[i](all_out)

        logits = self.logits(all_out)
        self._value_out = self.values_out(all_out)

        memory_outs = all_out[2:]
        # If memory_tau > max_seq_len -> overlap w/ previous `memory` input.
        if self.memory_tau > self.max_seq_len:
            memory_outs = [
                torch.cat(
                    [memory[i][:, -(self.memory_tau - self.max_seq_len):], m],
                    axis=1) for i, m in enumerate(memory_outs)
            ]
        else:
            memory_outs = [m[:, -self.memory_tau:] for m in memory_outs]

        T = list(inputs.size())[1]  # Length of input segment (time).

        # Postprocessing final output.
        logits = logits[:, -T:]
        self._value_out = self._value_out[:, -T:]

        return logits, [observations] + memory_outs

    @override(RecurrentNetwork)
    def get_initial_state(self) -> List[np.ndarray]:
        # State is the T last observations concat'd together into one Tensor.
        # Plus all Transformer blocks' E(l) outputs concat'd together (up to
        # tau timesteps).
        return [np.zeros((self.max_seq_len, self.obs_dim), np.float32)] + \
               [np.zeros((self.memory_tau, self.attn_dim), np.float32)
                for _ in range(self.num_transformer_units)]

    @override(ModelV2)
    def value_function(self) -> TensorType:
        return torch.reshape(self._value_out, [-1])
