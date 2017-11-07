from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.pytorch.model import Model
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
            layers.append(nn.Linear(last_layer_size, size))
            layers.append(activation())
            last_layer_size = size

        self.hidden_layers = nn.Sequential(*layers)

        self.logits = nn.Linear(last_layer_size, num_outputs)
        self.probs = nn.Softmax()
        self.value_branch = nn.Linear(last_layer_size, 1)

    def forward(self, x):
        res = self.hidden_layers(x)
        logits = self.logits(res)
        value = self.value_branch(res)
        return logits, value, None
