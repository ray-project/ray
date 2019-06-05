from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.model import restore_original_dimensions


# Experimental
class ModelV2(object):
    def __init__(self, obs_space, action_space, num_outputs, options):
        self.obs_space = obs_space
        if hasattr(obs_space, "original_space"):
            self.original_space = self.obs_space.original_space
        else:
            self.original_space = None
        self.action_space = action_space
        self.num_outputs = num_outputs
        self.options = options

    def get_initial_state(self):
        return []

    def __call__(self, input_dict, state, seq_lens):
        """Wrapper around forward() that unpacks obs."""
        if self.original_space:
            restored = input_dict.copy()
            restored["obs"] = restore_original_dimensions(
                input_dict["obs"], self.obs_space)
            restored["obs_flat"] = input_dict["obs"]
        return self.forward(restored, state, seq_lens)

    def forward(self, input_dict, state, seq_lens):
        """Returns (logits, features, state)."""
        raise NotImplementedError

    def get_branch_output(self, branch_type, num_outputs, feature_layer=None):
        raise NotImplementedError
