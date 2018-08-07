from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.pytorch.model import Model, SlimFC
from ray.rllib.models.pytorch.misc import normc_initializer
import torch.nn as nn


class FullyConnectedNetwork(Model):
    """Generic fully connected network, pytorch backend."""

    def _build_layers(self, inputs, num_outputs, options):
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
            layers.append(
                SlimFC(
                    in_size=last_layer_size,
                    out_size=size,
                    initializer=normc_initializer(1.0),
                    activation_fn=activation))
            last_layer_size = size

        self.hidden_layers = nn.Sequential(*layers)
        self.output = SlimFC(
            in_size=last_layer_size,
            out_size=num_outputs,
            initializer=normc_initializer(0.01),
            activation_fn=None)
        self.last_layer_size = last_layer_size


    def forward(self, obs):
        """ Internal method - pass in torch tensors, not numpy arrays

        Args:
            obs: observations and features

        Return:
            output: result of a forward pass of obs through the model
            last_hidden_activations: activations after the last hidden layer"""

        last_hidden_activations = self.hidden_layers(obs)
        return self.output(last_hidden_activations), last_hidden_activations
