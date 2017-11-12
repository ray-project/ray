from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.pytorch.model import Model, SlimFC
from ray.rllib.models.pytorch.misc import normc_initializer
import torch.nn as nn


class FullyConnectedNetwork(Model):
    """TODO(rliaw): Logits, Value should both be contained here"""
    def _init(self, inputs, num_outputs, options):
        assert type(inputs) is int
        hiddens = options.get("fcnet_hiddens", [256, 256])
        fcnet_activation = options.get("fcnet_activation", "tanh")
        activation = None
        if fcnet_activation == "tanh":
            activation = nn.Tanh
        elif fcnet_activation == "relu":
            activation = nn.ReLU
        print("Constructing fcnet {} {}".format(hiddens, activation))

        layers = []
        last_layer_size = inputs
        for size in hiddens:
            layers.append(SlimFC(
                last_layer_size, size,
                initializer=normc_initializer(1.0),
                activation_fn=activation))
            last_layer_size = size

        self.hidden_layers = nn.Sequential(*layers)

        self.logits = SlimFC(
            last_layer_size, num_outputs,
            initializer=normc_initializer(0.01),
            activation_fn=None)
        self.probs = nn.Softmax()
        self.value_branch = SlimFC(
            last_layer_size, 1,
            initializer=normc_initializer(1.0),
            activation_fn=None)

    def forward(self, obs):
        """ Internal method - pass in Variables, not numpy arrays

        Args:
            obs: observations and features

        Return:
            logits: logits to be sampled from for each state
            value: value function for each state"""
        res = self.hidden_layers(obs)
        logits = self.logits(res)
        value = self.value_branch(res)
        return logits, value
