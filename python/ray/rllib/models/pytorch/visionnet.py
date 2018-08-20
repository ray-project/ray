from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch.nn as nn

from ray.rllib.models.pytorch.model import Model, SlimConv2d, SlimFC
from ray.rllib.models.pytorch.misc import normc_initializer, valid_padding


class VisionNetwork(Model):
    """Generic vision network"""

    def _build_layers(self, inputs, num_outputs, options):
        """TF visionnet in PyTorch.

        Params:
            inputs (tuple): (channels, rows/height, cols/width)
            num_outputs (int): logits size
        """
        filters = options.get("conv_filters", [
            [16, [8, 8], 4],
            [32, [4, 4], 2],
            [512, [11, 11], 1],
        ])
        layers = []
        in_channels, in_size = inputs[0], inputs[1:]

        for out_channels, kernel, stride in filters[:-1]:
            padding, out_size = valid_padding(in_size, kernel,
                                              [stride, stride])
            layers.append(
                SlimConv2d(in_channels, out_channels, kernel, stride, padding))
            in_channels = out_channels
            in_size = out_size

        out_channels, kernel, stride = filters[-1]
        layers.append(
            SlimConv2d(in_channels, out_channels, kernel, stride, None))
        self._convs = nn.Sequential(*layers)

        self.logits = SlimFC(
            out_channels, num_outputs, initializer=nn.init.xavier_uniform_)
        self.value_branch = SlimFC(
            out_channels, 1, initializer=normc_initializer())

    def hidden_layers(self, obs):
        """ Internal method - pass in torch tensors, not numpy arrays

        args:
            obs: observations and features"""
        res = self._convs(obs)
        res = res.squeeze(3)
        res = res.squeeze(2)
        return res

    def forward(self, obs):
        """Internal method. Implements the

        Args:
            obs (PyTorch): observations and features

        Return:
            logits (PyTorch): logits to be sampled from for each state
            value (PyTorch): value function for each state"""
        res = self.hidden_layers(obs)
        logits = self.logits(res)
        value = self.value_branch(res).squeeze(1)
        return logits, value
