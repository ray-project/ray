from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np
import torch.nn as nn

from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.misc import normc_initializer, SlimFC, \
    _get_activation_fn
from ray.rllib.utils.annotations import override

logger = logging.getLogger(__name__)


class FullyConnectedNetwork(TorchModelV2, nn.Module):
    """Generic fully connected network."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)

        hiddens = model_config.get("fcnet_hiddens")
        activation = _get_activation_fn(model_config.get("fcnet_activation"))
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
        self._cur_value = None

    @override(TorchModelV2)
    def forward(self, input_dict, state, seq_lens):
        obs = input_dict["obs_flat"]
        features = self._hidden_layers(obs.reshape(obs.shape[0], -1))
        logits = self._logits(features)
        self._cur_value = self._value_branch(features).squeeze(1)
        return logits, state

    @override(TorchModelV2)
    def value_function(self):
        assert self._cur_value is not None, "must call forward() first"
        return self._cur_value
