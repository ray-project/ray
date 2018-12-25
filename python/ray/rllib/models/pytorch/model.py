from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch
import torch.nn as nn

from ray.rllib.models.model import _restore_original_dimensions


class TorchModel(nn.Module):
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
        """Wraps _forward() to unpack flattened Dict and Tuple observations."""
        input_dict["obs"] = input_dict["obs"].float()  # force float?
        input_dict = _restore_original_dimensions(
            input_dict, self.obs_space, tensorlib=torch)
        return self._forward(input_dict, hidden_state)

    def state_init(self):
        """Returns the initial hidden state, if any."""
        return []

    def _forward(self, input_dict, hidden_state):
        """Forward pass for the model.

        Prefer implementing this instead of forward() directly for proper
        handling of Dict and Tuple observations.

        Arguments:
            input_dict (dict): Dictionary of tensor inputs, commonly
                including "obs", "prev_action", "prev_reward", each of shape
                [BATCH_SIZE, ...].
            hidden_state (list): List of hidden state tensors, each of shape
                [BATCH_SIZE, h_size].

        Returns:
            (outputs, feature_layer, values, state): Tensors of size
                [BATCH_SIZE, num_outputs], [BATCH_SIZE, desired_feature_size],
                [BATCH_SIZE], and [len(hidden_state), BATCH_SIZE, h_size].
        """
        raise NotImplementedError
