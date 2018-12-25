from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch.nn as nn


class PyTorchModel(nn.Module):
    """Defines an abstract network model for use with RLlib / PyTorch."""

    def __init__(self, obs_space, num_outputs, options):
        """All custom RLlib torch models must support this constructor.

        Arguments:
            obs_space (gym.Space): Input observation space.
            num_outputs (int): Output tensor must be of size
                [BATCH_SIZE, num_outputs].
            options (dict): Dictionary of model options.
        """
        nn.Module.__init__(self)
        self.obs_space = obs_space
        self.num_outputs = num_outputs
        self.options = options

    def forward(self, input_dict, hidden_state):
        """Forward pass for the model.

        Arguments:
            input_dict (dict): Dictionary of tensor inputs, commonly
                including "obs", "prev_action", "prev_reward", each of shape
                [BATCH_SIZE, ...].
            hidden_state (list): List of hidden state tensors, each of shape
                [BATCH_SIZE, h_size].

        Returns:
            (outputs, feature_layer, state_out): Tensors of size
                [BATCH_SIZE, num_outputs], [BATCH_SIZE, desired_feature_size],
                and [len(hidden_state), BATCH_SIZE, h_size].
        """
        raise NotImplementedError
