from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch.nn as nn


class Model(nn.Module):
    def __init__(self, obs_space, ac_space, options):
        super(Model, self).__init__()
        self._init(obs_space, ac_space, options)

    def _init(self, inputs, num_outputs, options):
        raise NotImplementedError

    def forward(self, obs):
        """Forward pass for the model. Internal method - should only
        be passed PyTorch Tensors.

        PyTorch automatically overloads the given model
        with this function. Recommended that model(obs)
        is used instead of model.forward(obs). See
        https://discuss.pytorch.org/t/any-different-between-model
        -input-and-model-forward-input/3690
        """
        raise NotImplementedError
