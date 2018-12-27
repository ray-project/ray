from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from torch import nn
import torch.nn.functional as F


# TODO(ekl) we should have common models for pytorch like we do for TF
class RNNModel(nn.Module):
    def __init__(self, obs_size, rnn_hidden_dim, n_actions):
        nn.Module.__init__(self)
        self.rnn_hidden_dim = rnn_hidden_dim
        self.n_actions = n_actions
        self.fc1 = nn.Linear(obs_size, rnn_hidden_dim)
        self.rnn = nn.GRUCell(rnn_hidden_dim, rnn_hidden_dim)
        self.fc2 = nn.Linear(rnn_hidden_dim, n_actions)

    def init_hidden(self):
        # make hidden states on same device as model
        return self.fc1.weight.new(1, self.rnn_hidden_dim).zero_()

    def forward(self, inputs, hidden_state):
        x = F.relu(self.fc1(inputs.float()))
        h_in = hidden_state.reshape(-1, self.rnn_hidden_dim)
        h = self.rnn(x, h_in)
        q = self.fc2(h)
        return q, h
