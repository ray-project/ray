from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from torch import nn
import torch.nn.functional as F

from ray.rllib.models.preprocessors import get_preprocessor
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.annotations import override


class RNNModel(TorchModelV2):
    """The default RNN model for QMIX."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super(RNNModel, self).__init__(obs_space, action_space, num_outputs,
                                       model_config, name)
        self.obs_size = _get_size(obs_space)
        self.rnn_hidden_dim = model_config["lstm_cell_size"]
        self.fc1 = nn.Linear(self.obs_size, self.rnn_hidden_dim)
        self.rnn = nn.GRUCell(self.rnn_hidden_dim, self.rnn_hidden_dim)
        self.fc2 = nn.Linear(self.rnn_hidden_dim, num_outputs)

    @override(TorchModelV2)
    def get_initial_state(self):
        # make hidden states on same device as model
        return [self.fc1.weight.new(1, self.rnn_hidden_dim).zero_().squeeze(0)]

    @override(TorchModelV2)
    def forward(self, input_dict, hidden_state, seq_lens):
        x = F.relu(self.fc1(input_dict["obs_flat"].float()))
        h_in = hidden_state[0].reshape(-1, self.rnn_hidden_dim)
        h = self.rnn(x, h_in)
        q = self.fc2(h)
        return q, [h]


def _get_size(obs_space):
    return get_preprocessor(obs_space)(obs_space).size
