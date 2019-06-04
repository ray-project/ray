from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


# Experimental
class ModelV2(object):
    def __init__(self, obs_space, action_space, num_outputs, options):
        # TODO(ekl) handle nested spaces
        self.obs_space = obs_space
        self.action_space = action_space
        self.num_outputs = num_outputs
        self.options = options

    def get_initial_state(self):
        return None

    def forward(self, input_dict, state, seq_lens):
        """Returns (logits, features, state)."""
        raise NotImplementedError

    def get_branch_output(self, branch_type, num_outputs, feature_layer=None):
        raise NotImplementedError
