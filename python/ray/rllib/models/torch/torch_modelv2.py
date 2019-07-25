from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch.nn as nn

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class TorchModelV2(ModelV2, nn.Module):
    """Torch version of ModelV2.

    Note that this class by itself is not a valid model unless you
    implement forward() in a subclass."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        ModelV2.__init__(
            self,
            obs_space,
            action_space,
            num_outputs,
            model_config,
            name,
            framework="torch")
        nn.Module.__init__(self)
