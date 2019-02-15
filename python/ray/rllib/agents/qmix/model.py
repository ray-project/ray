from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from torch import nn
import torch.nn.functional as F

from ray.rllib.models.preprocessors import get_preprocessor
from ray.rllib.models.pytorch.model import TorchModel
from ray.rllib.utils.annotations import override


class RNNModel(TorchModel):
    """The default RNN model for QMIX."""

    def __init__(self, obs_space, num_outputs, options):
        TorchModel.__init__(self, obs_space, num_outputs, options)
        self.obs_size = _get_size(obs_space)
        self.rnn_hidden_dim = options["lstm_cell_size"]
        self.fc1 = nn.Linear(self.obs_size, self.rnn_hidden_dim)
        self.rnn = nn.GRUCell(self.rnn_hidden_dim, self.rnn_hidden_dim)
        self.fc2 = nn.Linear(self.rnn_hidden_dim, num_outputs)

    @override(TorchModel)
    def state_init(self):
        # make hidden states on same device as model
        return [self.fc1.weight.new(1, self.rnn_hidden_dim).zero_().squeeze(0)]

    @override(TorchModel)
    def _forward(self, input_dict, hidden_state):
        x = F.relu(self.fc1(input_dict["obs"]))
        h_in = hidden_state[0].reshape(-1, self.rnn_hidden_dim)
        h = self.rnn(x, h_in)
        q = self.fc2(h)
        return q, h, None, [h]


def _get_size(obs_space):
    return get_preprocessor(obs_space)(obs_space).size
