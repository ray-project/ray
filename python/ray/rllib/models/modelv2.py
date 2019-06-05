from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.model import restore_original_dimensions


class ModelV2(object):
    """Defines a Keras-style abstract network model for use with RLlib.

    This interface is used for both TF and Torch custom models. Experimental.

    Attributes:
        obs_space (Space): observation space of the target gym env. This
            may have an `original_space` attribute that specifies how to
            unflatten the tensor into a ragged tensor.
        action_space (Space): action space of the target gym env
            num_outputs (int): the size of the output vector of the model
        num_outputs (int): the size of the output vector of the model
        options (dict): options for the model, documented in ModelCatalog
    """

    def __init__(self, obs_space, action_space, num_outputs, options):
        """Initialize the model.

        This method should create any variables used by the model.
        """

        self.obs_space = obs_space
        self.action_space = action_space
        self.num_outputs = num_outputs
        self.options = options

    def get_initial_state(self):
        """Get the initial recurrent state values for the model.

        Returns:
            list of np.array objects, if any
        """
        return []

    def forward(self, input_dict, state, seq_lens):
        """Call the model with the given input tensors and state.

        Any complex observations (dicts, tuples, etc.) will be unpacked by
        __call__ before being passed to forward(). To access the flattened
        observation tensor, refer to input_dict["obs_flat"].

        Custom models should override this instead of __call__.
        """
        raise NotImplementedError

    def get_branch_output(self, branch_type, num_outputs, feature_layer=None):
        """Get the branch output of the model (e.g., "value" branch).

        Arguments:
            branch_type (str): identifier for the branch (e.g., "value")
            num_outputs (int): size of the branch output
            feature_layer (tensor): if specified, this hints that the branch
                output should be computed using this shared feature layer.
                However custom models are free to ignore this hint.

        Returns:
            tensor: branch output of size [BATCH, num_outputs]
        """
        raise NotImplementedError

    def __call__(self, input_dict, state, seq_lens):
        """Call the model with the given input tensors and state.

        Arguments:
            input_dict (dict): dictionary of input tensors, including "obs",
                "prev_action", "prev_reward", "is_training"
            state (list): list of state tensors with sizes matching those
                returned by get_initial_state + the batch dimension
            seq_lens (list): 1d tensor holding input sequence lengths

        Returns:
            (outputs, state, feature_layer): The model output tensor of size
                [BATCH, num_outputs], a list of state tensors of sizes
                [BATCH, state_size_i], and a tensor of [BATCH, feature_size]
        """

        if hasattr(self.obs_space, "original_space"):
            restored = input_dict.copy()
            restored["obs"] = restore_original_dimensions(
                input_dict["obs"], self.obs_space)
            restored["obs_flat"] = input_dict["obs"]
        return self.forward(restored, state, seq_lens)
