from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.modelv2 import ModelV2


class TorchModelV2(ModelV2):
    """Torch version of ModelV2."""

    def __init__(self, obs_space, action_space, output_spec, model_config,
                 name):
        ModelV2.__init__(
            self,
            obs_space,
            action_space,
            output_spec,
            model_config,
            name,
            framework="torch")
