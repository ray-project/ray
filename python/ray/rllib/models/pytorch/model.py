from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch.nn as nn


class PyTorchModel(nn.Module):
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


class SlimConv2d(PyTorchModel):
    """Simple mock of tf.slim Conv2d"""

    def __init__(self,
                 in_channels,
                 out_channels,
                 kernel,
                 stride,
                 padding,
                 initializer=nn.init.xavier_uniform_,
                 activation_fn=nn.ReLU,
                 bias_init=0):
        super(SlimConv2d, self).__init__()
        layers = []
        if padding:
            layers.append(nn.ZeroPad2d(padding))
        conv = nn.Conv2d(in_channels, out_channels, kernel, stride)
        if initializer:
            initializer(conv.weight)
        nn.init.constant_(conv.bias, bias_init)

        layers.append(conv)
        if activation_fn:
            layers.append(activation_fn())
        self._model = nn.Sequential(*layers)

    def forward(self, x):
        return self._model(x)


class SlimFC(PyTorchModel):
    """Simple PyTorch of `linear` function"""

    def __init__(self,
                 in_size,
                 out_size,
                 initializer=None,
                 activation_fn=None,
                 bias_init=0):
        super(SlimFC, self).__init__()
        layers = []
        linear = nn.Linear(in_size, out_size)
        if initializer:
            initializer(linear.weight)
        nn.init.constant_(linear.bias, bias_init)
        layers.append(linear)
        if activation_fn:
            layers.append(activation_fn())
        self._model = nn.Sequential(*layers)

    def forward(self, x):
        return self._model(x)
