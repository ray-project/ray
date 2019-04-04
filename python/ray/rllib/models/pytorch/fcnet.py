from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np
import torch.nn as nn

from ray.rllib.models.pytorch.model import TorchModel
from ray.rllib.models.pytorch.misc import normc_initializer, SlimFC, \
    _get_activation_fn
from ray.rllib.utils.annotations import override

logger = logging.getLogger(__name__)


class FullyConnectedNetwork(TorchModel):
    """Generic fully connected network."""

    def __init__(self, obs_space, num_outputs, options):
        TorchModel.__init__(self, obs_space, num_outputs, options)
        hiddens = options.get("fcnet_hiddens")
        activation = _get_activation_fn(options.get("fcnet_activation"))
        logger.debug("Constructing fcnet {} {}".format(hiddens, activation))
        layers = []
        last_layer_size = np.product(obs_space.shape)
        for size in hiddens:
            layers.append(
                SlimFC(
                    in_size=last_layer_size,
                    out_size=size,
                    initializer=normc_initializer(1.0),
                    activation_fn=activation))
            last_layer_size = size

        self._hidden_layers = nn.Sequential(*layers)

        self._logits = SlimFC(
            in_size=last_layer_size,
            out_size=num_outputs,
            initializer=normc_initializer(0.01),
            activation_fn=None)
        self._value_branch = SlimFC(
            in_size=last_layer_size,
            out_size=1,
            initializer=normc_initializer(1.0),
            activation_fn=None)

    @override(nn.Module)
    def forward(self, input_dict, hidden_state):
        # Note that we override forward() and not _forward() to get the
        # flattened obs here.
        obs = input_dict["obs"]
        features = self._hidden_layers(obs.reshape(obs.shape[0], -1))
        logits = self._logits(features)
        value = self._value_branch(features).squeeze(1)
        return logits, features, value, hidden_state
