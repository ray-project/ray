from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


@PublicAPI
class TFModelV2(ModelV2):
    """TF version of ModelV2.

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
            framework="tf")
        self.var_list = []

    def update_ops(self):
        """Return the list of update ops for this model.

        For example, this should include any BatchNorm update ops."""
        return []

    def register_variables(self, variables):
        """Register the given list of variables with this model."""
        self.var_list.extend(variables)

    def variables(self):
        """Returns the list of variables for this model."""
        return list(self.var_list)

    def trainable_variables(self):
        """Returns the list of trainable variables for this model."""
        return [v for v in self.variables() if v.trainable]
