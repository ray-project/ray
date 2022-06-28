import numpy as np
import gym
from gym.spaces import Discrete, MultiDiscrete
import tree  # pip install dm_tree
from typing import Dict, List, Tuple, Union

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.torch_utils import flatten_inputs_to_1d_tensor, one_hot
from ray.rllib.utils.typing import ModelConfigDict, TensorType
from ray.rllib.models.torch.recurrent_net import RecurrentNetwork
from ray.rllib.models.torch.modules.relative_multi_head_attention import (
    RelativePositionEmbedding,
)

torch, nn = try_import_torch()

try:
    from fast_transformers import builders
except ImportError:
    raise ImportError(
        "Linear transformer library not installed. "
        "Try `pip install pytorch-fast-transformers`"
    )


class LinearAttentionWrapper(RecurrentNetwork, nn.Module):
    """An LSTM wrapper serving as an interface for ModelV2s that set use_lstm."""

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

        # At this point, self.num_outputs is the number of nodes coming
        # from the wrapped (underlying) model. In other words, self.num_outputs
        # is the input size for the LSTM layer.
        # If None, set it to the observation space.
        if self.num_outputs is None:
            self.num_outputs = int(np.product(self.obs_space.shape))

        self.attn_hidden_size = model_config.get("linear_attn_hidden_size", 128)
        self.time_major = model_config.get("_time_major", False)
        if self.time_major:
            raise NotImplementedError()
        self.use_prev_action = model_config["lstm_use_prev_action"]
        self.use_prev_reward = model_config["lstm_use_prev_reward"]

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
        if self.use_prev_action:
            self.num_outputs += self.action_dim
        if self.use_prev_reward:
            self.num_outputs += 1

        # Define actual LSTM layer (with num_outputs being the nodes coming
        # from the wrapped (underlying) layer).
        self.embedding = RelativePositionEmbedding(self.num_outputs)
        builder = builders.RecurrentEncoderBuilder()
        builder.attention_type = "linear"
        builder.n_layers = 1
        builder.n_heads = 1
        builder.feed_forward_dimensions = self.attn_hidden_size
        builder.query_dimensions = self.num_outputs
        # The value_dimensions determines the input shape of the xformer
        builder.value_dimensions = self.num_outputs
        builder.dropout = 0
        self.xformer = builder.get()
        # self.lstm = nn.LSTM(
        #    self.num_outputs, self.cell_size, batch_first=not self.time_major
        # )

        # Set self.num_outputs to the number of output nodes desired by the
        # caller of this constructor.
        self.num_inputs = self.num_outputs
        self.num_outputs = num_outputs

        # Postprocess LSTM output with another hidden layer and compute values.
        self._logits_branch = SlimFC(
            in_size=self.num_inputs,
            out_size=self.num_outputs,
            activation_fn=None,
            initializer=torch.nn.init.xavier_uniform_,
        )
        self._value_branch = SlimFC(
            in_size=self.num_inputs,
            out_size=1,
            activation_fn=None,
            initializer=torch.nn.init.xavier_uniform_,
        )

        # __sphinx_doc_begin__
        # Add prev-a/r to this model's view, if required.
        if model_config["lstm_use_prev_action"]:
            self.view_requirements[SampleBatch.PREV_ACTIONS] = ViewRequirement(
                SampleBatch.ACTIONS, space=self.action_space, shift=-1
            )
        if model_config["lstm_use_prev_reward"]:
            self.view_requirements[SampleBatch.PREV_REWARDS] = ViewRequirement(
                SampleBatch.REWARDS, shift=-1
            )
        # __sphinx_doc_end__

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
        if self.model_config["lstm_use_prev_action"]:
            prev_a = input_dict[SampleBatch.PREV_ACTIONS]
            # If actions are not processed yet (in their original form as
            # have been sent to environment):
            # Flatten/one-hot into 1D array.
            if self.model_config["_disable_action_flattening"]:
                prev_a_r.append(
                    flatten_inputs_to_1d_tensor(
                        prev_a, spaces_struct=self.action_space_struct, time_axis=False
                    )
                )
            # If actions are already flattened (but not one-hot'd yet!),
            # one-hot discrete/multi-discrete actions here.
            else:
                if isinstance(self.action_space, (Discrete, MultiDiscrete)):
                    prev_a = one_hot(prev_a.float(), self.action_space)
                else:
                    prev_a = prev_a.float()
                prev_a_r.append(torch.reshape(prev_a, [-1, self.action_dim]))
        # Prev rewards.
        if self.model_config["lstm_use_prev_reward"]:
            prev_a_r.append(
                torch.reshape(input_dict[SampleBatch.PREV_REWARDS].float(), [-1, 1])
            )

        # Concat prev. actions + rewards to the "main" input.
        if prev_a_r:
            wrapped_out = torch.cat([wrapped_out] + prev_a_r, dim=1)

        # Push everything through our LSTM.
        input_dict["obs_flat"] = wrapped_out
        return super().forward(input_dict, state, seq_lens)

    @override(RecurrentNetwork)
    def forward_rnn(
        self, inputs: TensorType, state: List[TensorType], seq_lens: TensorType
    ) -> Tuple[TensorType, List[TensorType]]:
        B = seq_lens.numel()
        self._features = torch.zeros(
            (B, seq_lens.max(), self.num_inputs), device=inputs.device
        )
        si, zi, count = state
        # import ray; ray.util.pdb.set_trace()
        for t in range(seq_lens.max()):
            # Mask out right-justified zero-padding
            batch_mask = t < seq_lens
            feat, [[si_out, zi_out]] = self.xformer(
                inputs[batch_mask, t].clone(),
                [[si[batch_mask].clone(), zi[batch_mask].clone()]],
            )
            # if self.training:
            # import ray; ray.util.pdb.set_trace()
            self._features[batch_mask, t] = feat.reshape(
                batch_mask.nonzero().numel(), -1
            )
            # Fucking autograd
            si = si.clone()
            zi = zi.clone()
            si[batch_mask] = si_out.clone()
            zi[batch_mask] = zi_out.clone()
        count = count + seq_lens
        state = [si, zi, count]
        model_out = self._logits_branch(self._features.clone())
        # print(B, seq_lens.max(), model_out.shape)
        return model_out, state

    @override(ModelV2)
    def get_initial_state(self) -> Union[List[np.ndarray], List[TensorType]]:
        # Place hidden states on same device as model.
        si = torch.zeros(1, self.num_inputs, self.num_inputs)
        zi = torch.zeros(1, self.num_inputs)
        count = torch.zeros(1)
        return [si, zi, count]

    @override(ModelV2)
    def value_function(self) -> TensorType:
        assert self._features is not None, "must call forward() first"
        return torch.reshape(self._value_branch(self._features), [-1])
