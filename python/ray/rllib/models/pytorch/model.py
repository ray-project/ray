from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch.nn as nn


class Model(nn.Module):
    def __init__(self, obs_space, ac_space, options):
        super(Model, self).__init__()
        self.volatile = False
        self._init(obs_space, ac_space, options)

    def _init(self, inputs, num_outputs, options):
        raise NotImplementedError
