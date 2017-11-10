from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch.nn as nn

from ray.rllib.models.pytorch.model import Model


class VisionNetwork(Model):
    """Generic vision network."""

    def _init(self, inputs, num_outputs, options):
        filters = options.get("conv_filters", [
            [16, 8, 4],
            [32, 4, 2]
        ])
        layers = []
        input_channels = inputs[0]
        for out_size, kernel, stride in filters:
            layers.append(nn.Conv2d(
                input_channels, out_size, kernel, stride))
            input_channels = out_size

        out_size = 512
        self._convs = nn.Sequential(*layers)

        # TODO(rliaw): This should definitely not be hardcoded
        self.fc1 = nn.Linear(32*8*8, out_size)
        self.logits = nn.Linear(out_size, num_outputs)
        self.probs = nn.Softmax()
        self.value_branch = nn.Linear(out_size, 1)
        self.value_branch.weight.data.normal_(0, 1)
        self.value_branch.weight.data *= 1 / torch.sqrt(
            m.weight.data.pow(2).sum(1, keepdim=True))

    def hidden_layers(self, obs):
        """ Internal method - pass in Variables, not numpy arrays

        args:
            obs: observations and features"""
        res = self._convs(obs)
        res = res.view(-1, 32*8*8)
        return self.fc1(res)

    def forward(self, obs):
        """Internal method. Implements the

        Args:
            obs (PyTorch): observations and features

        Return:
            logits (PyTorch): logits to be sampled from for each state
            value (PyTorch): value function for each state"""
        res = self.hidden_layers(obs)
        logits = self.logits(res)
        value = self.value_branch(res)
        return logits, value
