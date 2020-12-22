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
from gym.spaces import Box

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.modules import GRUGate, \
    RelativeMultiHeadAttention, SkipConnection
from ray.rllib.models.torch.recurrent_net import RecurrentNetwork
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModelConfigDict, TensorType, List

torch, nn = try_import_torch()


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
                 memory_inference: int,
                 memory_training: int,
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
            memory_inference (int): The number of timesteps to concat (time
                axis) and feed into the next transformer unit as inference
                input. The first transformer unit will receive this number of
                past observations (plus the current one), instead.
            memory_training (int): The number of timesteps to concat (time
                axis) and feed into the next transformer unit as training
                input (plus the actual input sequence of len=max_seq_len).
                The first transformer unit will receive this number of
                past observations (plus the input sequence), instead.
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
        self.memory_inference = memory_inference
        self.memory_training = memory_training
        self.head_dim = head_dim
        self.max_seq_len = model_config["max_seq_len"]
        self.obs_dim = observation_space.shape[0]

        self.linear_layer = SlimFC(
            in_size=self.obs_dim, out_size=self.attn_dim)

        self.layers = [self.linear_layer]

        attention_layers = []
        # 2) Create L Transformer blocks according to [2].
        for i in range(self.num_transformer_units):
            # RelativeMultiHeadAttention part.
            MHA_layer = SkipConnection(
                RelativeMultiHeadAttention(
                    in_dim=self.attn_dim,
                    out_dim=self.attn_dim,
                    num_heads=num_heads,
                    head_dim=head_dim,
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

            # Build a list of all attanlayers in order.
            attention_layers.extend([MHA_layer, E_layer])

        # Create a Sequential such that all parameters inside the attention
        # layers are automatically registered with this top-level model.
        self.attention_layers = nn.Sequential(*attention_layers)
        self.layers.extend(attention_layers)

        # Postprocess GTrXL output with another hidden layer.
        self.logits = SlimFC(
            in_size=self.attn_dim,
            out_size=self.num_outputs,
            activation_fn=nn.ReLU)

        # Value function used by all RLlib Torch RL implementations.
        self._value_out = None
        self.values_out = SlimFC(
            in_size=self.attn_dim, out_size=1, activation_fn=None)

        # Setup inference view (`memory-inference` x past observations +
        # current one (0))
        # 1 to `num_transformer_units`: Memory data (one per transformer unit).
        for i in range(self.num_transformer_units):
            space = Box(-1.0, 1.0, shape=(self.attn_dim, ))
            self.inference_view_requirements["state_in_{}".format(i)] = \
                ViewRequirement(
                    "state_out_{}".format(i),
                    shift="-{}:-1".format(self.memory_inference),
                    # Repeat the incoming state every max-seq-len times.
                    batch_repeat_value=self.max_seq_len,
                    space=space)
            self.inference_view_requirements["state_out_{}".format(i)] = \
                ViewRequirement(
                    space=space,
                    used_for_training=False)

    @override(ModelV2)
    def forward(self, input_dict, state: List[TensorType],
                seq_lens: TensorType) -> (TensorType, List[TensorType]):
        assert seq_lens is not None

        # Add the needed batch rank (tf Models' Input requires this).
        observations = input_dict[SampleBatch.OBS]
        # Add the time dim to observations.
        B = len(seq_lens)
        T = observations.shape[0] // B
        observations = torch.reshape(observations,
                                     [-1, T] + list(observations.shape[1:]))

        all_out = observations
        memory_outs = []
        for i in range(len(self.layers)):
            # MHA layers which need memory passed in.
            if i % 2 == 1:
                all_out = self.layers[i](all_out, memory=state[i // 2])
            # Either self.linear_layer (initial obs -> attn. dim layer) or
            # MultiLayerPerceptrons. The output of these layers is always the
            # memory for the next forward pass.
            else:
                all_out = self.layers[i](all_out)
                memory_outs.append(all_out)

        # Discard last output (not needed as a memory since it's the last
        # layer).
        memory_outs = memory_outs[:-1]

        logits = self.logits(all_out)
        self._value_out = self.values_out(all_out)

        return torch.reshape(logits, [-1, self.num_outputs]), [
            torch.reshape(m, [-1, self.attn_dim]) for m in memory_outs
        ]

    # TODO: (sven) Deprecate this once trajectory view API has fully matured.
    @override(RecurrentNetwork)
    def get_initial_state(self) -> List[np.ndarray]:
        return []

    @override(ModelV2)
    def value_function(self) -> TensorType:
        return torch.reshape(self._value_out, [-1])
