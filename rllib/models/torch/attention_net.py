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

from ray.rllib.models.torch.misc import normc_initializer as normc_init_torch
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.recurrent_net import RecurrentNetwork
from ray.rllib.utils.framework import try_import_tf, try_import_torch

torch, nn = try_import_torch()

class GTrXLNet(RecurrentNetwork):
    """A GTrXL net Model described in [2]."""

    #methods copied over from torch recurrentnet, all still need to be implemented
    def __init__(self,
                 obs_space,
                 action_space,
                 num_outputs,
                 model_config,
                 name,
                 fc_size=64,
                 lstm_state_size=256):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)

        self.obs_size = get_preprocessor(obs_space)(obs_space).size
        self.fc_size = fc_size
        self.lstm_state_size = lstm_state_size

        # Build the Module from fc + LSTM + 2xfc (action + value outs).
        self.fc1 = nn.Linear(self.obs_size, self.fc_size)
        self.lstm = nn.LSTM(
            self.fc_size, self.lstm_state_size, batch_first=True)
        self.action_branch = nn.Linear(self.lstm_state_size, num_outputs)
        self.value_branch = nn.Linear(self.lstm_state_size, 1)
        # Holds the current "base" output (before logits layer).
        self._features = None


    @override(ModelV2)
    def get_initial_state(self):
        # Place hidden states on same device as model.
        h = [
            self.fc1.weight.new(1, self.lstm_state_size).zero_().squeeze(0),
            self.fc1.weight.new(1, self.lstm_state_size).zero_().squeeze(0)
        ]
        return h

    @override(ModelV2)
    def value_function(self):
        assert self._features is not None, "must call forward() first"
        return torch.reshape(self.value_branch(self._features), [-1])

    @override(TorchRNN)
    def forward_rnn(self, inputs, state, seq_lens):
        """Feeds `inputs` (B x T x ..) through the Gru Unit.

        Returns the resulting outputs as a sequence (B x T x ...).
        Values are stored in self._cur_value in simple (B) shape (where B
        contains both the B and T dims!).

        Returns:
            NN Outputs (B x T x ...) as sequence.
            The state batches as a List of two items (c- and h-states).
        """
        x = nn.functional.relu(self.fc1(inputs))
        self._features, [h, c] = self.lstm(
            x, [torch.unsqueeze(state[0], 0),
                torch.unsqueeze(state[1], 0)])
        action_out = self.action_branch(self._features)
        return action_out, [torch.squeeze(h, 0), torch.squeeze(c, 0)]



class RelativeMultiHeadAttention(TorchModelV2, nn.Module):
    def __init__(self,
                 out_dim,
                 num_heads,
                 head_dim,
                 rel_pos_encoder,
                 input_layernorm=False,
                 output_activation=None,
                 **kwargs):
        TorchModelV2.__init__(self, **kwargs)
        nn.Module.__init__(self)

        self.out_dim = out_dim
        self.num_heads = num_heads
        self.head_dim = head_dim
        self.rel_pos_encoder = rel_pos_encoder
        self.input_layernorm = input_layernorm
        self.output_activation = output_activation

        # TODO check the shape of the input
        self.qkv_layer = nn.Linear(self.inputs, 3*num_heads*head_dim)
        self.linear_layer = nn.Linear(3*num_heads*head_dim, out_dim)

        self._uvar = torch.ones((num_heads, head_dim), dtype=torch.float32)
        self._vvar = torch.ones((num_heads, head_dim), dtype=torch.float32)

        #TODO what are dimensions
        self._pos_proj = nn.Linear(num_heads*head_dim)

        self._input_layernorm = None
        if input_layernorm:
            self._input_layernorm = nn.LayerNorm(input.size())
        #then call self._input_layernorm(input)

    def forward(self, ):
#        self._hidden_layers = nn.Sequential(...)
#        self._logits = ...
#        self._value_branch = ...
