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
import gym
from gym.spaces import Box, Discrete, MultiDiscrete
import numpy as np
import tree  # pip install dm_tree
from typing import Dict, Optional, Union

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.modules import (
    GRUGate,
    RelativeMultiHeadAttention,
    SkipConnection,
)
from ray.rllib.models.torch.recurrent_net import RecurrentNetwork
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.torch_utils import flatten_inputs_to_1d_tensor, one_hot
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
        >>     attention_dim=32,
        >>     num_heads=2,
        >>     memory_tau=50,
        >>     etc..
        >> }
    """

    def __init__(
        self,
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
        init_gru_gate_bias: float = 2.0
    ):
        """Initializes a GTrXLNet.

        Args:
            num_transformer_units: The number of Transformer repeats to
                use (denoted L in [2]).
            attention_dim: The input and output dimensions of one
                Transformer unit.
            num_heads: The number of attention heads to use in parallel.
                Denoted as `H` in [3].
            memory_inference: The number of timesteps to concat (time
                axis) and feed into the next transformer unit as inference
                input. The first transformer unit will receive this number of
                past observations (plus the current one), instead.
            memory_training: The number of timesteps to concat (time
                axis) and feed into the next transformer unit as training
                input (plus the actual input sequence of len=max_seq_len).
                The first transformer unit will receive this number of
                past observations (plus the input sequence), instead.
            head_dim: The dimension of a single(!) attention head within
                a multi-head attention unit. Denoted as `d` in [3].
            position_wise_mlp_dim: The dimension of the hidden layer
                within the position-wise MLP (after the multi-head attention
                block within one Transformer unit). This is the size of the
                first of the two layers within the PositionwiseFeedforward. The
                second layer always has size=`attention_dim`.
            init_gru_gate_bias: Initial bias values for the GRU gates
                (two GRUs per Transformer unit, one after the MHA, one after
                the position-wise MLP).
        """

        super().__init__(
            observation_space, action_space, num_outputs, model_config, name
        )

        nn.Module.__init__(self)

        self.num_transformer_units = num_transformer_units
        self.attention_dim = attention_dim
        self.num_heads = num_heads
        self.memory_inference = memory_inference
        self.memory_training = memory_training
        self.head_dim = head_dim
        self.max_seq_len = model_config["max_seq_len"]
        self.obs_dim = observation_space.shape[0]

        self.linear_layer = SlimFC(in_size=self.obs_dim, out_size=self.attention_dim)

        self.layers = [self.linear_layer]

        attention_layers = []
        # 2) Create L Transformer blocks according to [2].
        for i in range(self.num_transformer_units):
            # RelativeMultiHeadAttention part.
            MHA_layer = SkipConnection(
                RelativeMultiHeadAttention(
                    in_dim=self.attention_dim,
                    out_dim=self.attention_dim,
                    num_heads=num_heads,
                    head_dim=head_dim,
                    input_layernorm=True,
                    output_activation=nn.ReLU,
                ),
                fan_in_layer=GRUGate(self.attention_dim, init_gru_gate_bias),
            )

            # Position-wise MultiLayerPerceptron part.
            E_layer = SkipConnection(
                nn.Sequential(
                    torch.nn.LayerNorm(self.attention_dim),
                    SlimFC(
                        in_size=self.attention_dim,
                        out_size=position_wise_mlp_dim,
                        use_bias=False,
                        activation_fn=nn.ReLU,
                    ),
                    SlimFC(
                        in_size=position_wise_mlp_dim,
                        out_size=self.attention_dim,
                        use_bias=False,
                        activation_fn=nn.ReLU,
                    ),
                ),
                fan_in_layer=GRUGate(self.attention_dim, init_gru_gate_bias),
            )

            # Build a list of all attanlayers in order.
            attention_layers.extend([MHA_layer, E_layer])

        # Create a Sequential such that all parameters inside the attention
        # layers are automatically registered with this top-level model.
        self.attention_layers = nn.Sequential(*attention_layers)
        self.layers.extend(attention_layers)

        # Final layers if num_outputs not None.
        self.logits = None
        self.values_out = None
        # Last value output.
        self._value_out = None
        # Postprocess GTrXL output with another hidden layer.
        if self.num_outputs is not None:
            self.logits = SlimFC(
                in_size=self.attention_dim,
                out_size=self.num_outputs,
                activation_fn=nn.ReLU,
            )

            # Value function used by all RLlib Torch RL implementations.
            self.values_out = SlimFC(
                in_size=self.attention_dim, out_size=1, activation_fn=None
            )
        else:
            self.num_outputs = self.attention_dim

        # Setup trajectory views (`memory-inference` x past memory outs).
        for i in range(self.num_transformer_units):
            space = Box(-1.0, 1.0, shape=(self.attention_dim,))
            self.view_requirements["state_in_{}".format(i)] = ViewRequirement(
                "state_out_{}".format(i),
                shift="-{}:-1".format(self.memory_inference),
                # Repeat the incoming state every max-seq-len times.
                batch_repeat_value=self.max_seq_len,
                space=space,
            )
            self.view_requirements["state_out_{}".format(i)] = ViewRequirement(
                space=space, used_for_training=False
            )

    @override(ModelV2)
    def forward(
        self, input_dict, state: List[TensorType], seq_lens: TensorType
    ) -> (TensorType, List[TensorType]):
        assert seq_lens is not None

        # Add the needed batch rank (tf Models' Input requires this).
        observations = input_dict[SampleBatch.OBS]
        # Add the time dim to observations.
        B = len(seq_lens)
        T = observations.shape[0] // B
        observations = torch.reshape(
            observations, [-1, T] + list(observations.shape[1:])
        )

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

        if self.logits is not None:
            out = self.logits(all_out)
            self._value_out = self.values_out(all_out)
            out_dim = self.num_outputs
        else:
            out = all_out
            out_dim = self.attention_dim

        return torch.reshape(out, [-1, out_dim]), [
            torch.reshape(m, [-1, self.attention_dim]) for m in memory_outs
        ]

    # TODO: (sven) Deprecate this once trajectory view API has fully matured.
    @override(RecurrentNetwork)
    def get_initial_state(self) -> List[np.ndarray]:
        return []

    @override(ModelV2)
    def value_function(self) -> TensorType:
        assert (
            self._value_out is not None
        ), "Must call forward first AND must have value branch!"
        return torch.reshape(self._value_out, [-1])


class AttentionWrapper(TorchModelV2, nn.Module):
    """GTrXL wrapper serving as interface for ModelV2s that set use_attention."""

    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        num_outputs: int,
        model_config: ModelConfigDict,
        name: str,
    ):

        nn.Module.__init__(self)
        super().__init__(obs_space, action_space, None, model_config, name)

        self.use_n_prev_actions = model_config["attention_use_n_prev_actions"]
        self.use_n_prev_rewards = model_config["attention_use_n_prev_rewards"]

        self.action_space_struct = get_base_struct_from_space(self.action_space)
        self.action_dim = 0

        for space in tree.flatten(self.action_space_struct):
            if isinstance(space, Discrete):
                self.action_dim += space.n
            elif isinstance(space, MultiDiscrete):
                self.action_dim += np.sum(space.nvec)
            elif space.shape is not None:
                self.action_dim += int(np.product(space.shape))
            else:
                self.action_dim += int(len(space))

        # Add prev-action/reward nodes to input to LSTM.
        if self.use_n_prev_actions:
            self.num_outputs += self.use_n_prev_actions * self.action_dim
        if self.use_n_prev_rewards:
            self.num_outputs += self.use_n_prev_rewards

        cfg = model_config

        self.attention_dim = cfg["attention_dim"]

        if self.num_outputs is not None:
            in_space = gym.spaces.Box(
                float("-inf"), float("inf"), shape=(self.num_outputs,), dtype=np.float32
            )
        else:
            in_space = obs_space

        # Construct GTrXL sub-module w/ num_outputs=None (so it does not
        # create a logits/value output; we'll do this ourselves in this wrapper
        # here).
        self.gtrxl = GTrXLNet(
            in_space,
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

        # Set final num_outputs to correct value (depending on action space).
        self.num_outputs = num_outputs

        # Postprocess GTrXL output with another hidden layer and compute
        # values.
        self._logits_branch = SlimFC(
            in_size=self.attention_dim,
            out_size=self.num_outputs,
            activation_fn=None,
            initializer=torch.nn.init.xavier_uniform_,
        )
        self._value_branch = SlimFC(
            in_size=self.attention_dim,
            out_size=1,
            activation_fn=None,
            initializer=torch.nn.init.xavier_uniform_,
        )

        self.view_requirements = self.gtrxl.view_requirements
        self.view_requirements["obs"].space = self.obs_space

        # Add prev-a/r to this model's view, if required.
        if self.use_n_prev_actions:
            self.view_requirements[SampleBatch.PREV_ACTIONS] = ViewRequirement(
                SampleBatch.ACTIONS,
                space=self.action_space,
                shift="-{}:-1".format(self.use_n_prev_actions),
            )
        if self.use_n_prev_rewards:
            self.view_requirements[SampleBatch.PREV_REWARDS] = ViewRequirement(
                SampleBatch.REWARDS, shift="-{}:-1".format(self.use_n_prev_rewards)
            )

    @override(RecurrentNetwork)
    def forward(
        self,
        input_dict: Dict[str, TensorType],
        state: List[TensorType],
        seq_lens: TensorType,
    ) -> (TensorType, List[TensorType]):
        assert seq_lens is not None
        # Push obs through "unwrapped" net's `forward()` first.
        wrapped_out, _ = self._wrapped_forward(input_dict, [], None)

        # Concat. prev-action/reward if required.
        prev_a_r = []

        # Prev actions.
        if self.use_n_prev_actions:
            prev_n_actions = input_dict[SampleBatch.PREV_ACTIONS]
            # If actions are not processed yet (in their original form as
            # have been sent to environment):
            # Flatten/one-hot into 1D array.
            if self.model_config["_disable_action_flattening"]:
                # Merge prev n actions into flat tensor.
                flat = flatten_inputs_to_1d_tensor(
                    prev_n_actions,
                    spaces_struct=self.action_space_struct,
                    time_axis=True,
                )
                # Fold time-axis into flattened data.
                flat = torch.reshape(flat, [flat.shape[0], -1])
                prev_a_r.append(flat)
            # If actions are already flattened (but not one-hot'd yet!),
            # one-hot discrete/multi-discrete actions here and concatenate the
            # n most recent actions together.
            else:
                if isinstance(self.action_space, Discrete):
                    for i in range(self.use_n_prev_actions):
                        prev_a_r.append(
                            one_hot(
                                prev_n_actions[:, i].float(), space=self.action_space
                            )
                        )
                elif isinstance(self.action_space, MultiDiscrete):
                    for i in range(
                        0, self.use_n_prev_actions, self.action_space.shape[0]
                    ):
                        prev_a_r.append(
                            one_hot(
                                prev_n_actions[
                                    :, i : i + self.action_space.shape[0]
                                ].float(),
                                space=self.action_space,
                            )
                        )
                else:
                    prev_a_r.append(
                        torch.reshape(
                            prev_n_actions.float(),
                            [-1, self.use_n_prev_actions * self.action_dim],
                        )
                    )
        # Prev rewards.
        if self.use_n_prev_rewards:
            prev_a_r.append(
                torch.reshape(
                    input_dict[SampleBatch.PREV_REWARDS].float(),
                    [-1, self.use_n_prev_rewards],
                )
            )

        # Concat prev. actions + rewards to the "main" input.
        if prev_a_r:
            wrapped_out = torch.cat([wrapped_out] + prev_a_r, dim=1)

        # Then through our GTrXL.
        input_dict["obs_flat"] = input_dict["obs"] = wrapped_out

        self._features, memory_outs = self.gtrxl(input_dict, state, seq_lens)
        model_out = self._logits_branch(self._features)
        return model_out, memory_outs

    @override(ModelV2)
    def get_initial_state(self) -> Union[List[np.ndarray], List[TensorType]]:
        return []

    @override(ModelV2)
    def value_function(self) -> TensorType:
        assert self._features is not None, "Must call forward() first!"
        return torch.reshape(self._value_branch(self._features), [-1])
