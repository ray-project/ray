from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.model import restore_original_dimensions
from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class ModelV2(object):
    """Defines a Keras-style abstract network model for use with RLlib.

    Custom models should extend either TFModelV2 or TorchModelV2 instead of
    this class directly.

    Data flow:
        obs -> forward() -> model_out
               value_function() -> V(s)

    Attributes:
        obs_space (Space): observation space of the target gym env. This
            may have an `original_space` attribute that specifies how to
            unflatten the tensor into a ragged tensor.
        action_space (Space): action space of the target gym env
        num_outputs (int): number of output units of the model
        model_config (dict): config for the model, documented in ModelCatalog
        name (str): name (scope) for the model
        framework (str): either "tf" or "torch"
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name, framework):
        """Initialize the model.

        This method should create any variables used by the model.
        """

        self.obs_space = obs_space
        self.action_space = action_space
        self.num_outputs = num_outputs
        self.model_config = model_config
        self.name = name or "default_model"
        self.framework = framework

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

        This method can be called any number of times. In eager execution,
        each call to forward() will eagerly evaluate the model. In symbolic
        execution, each call to forward creates a computation graph that
        operates over the variables of this model (i.e., shares weights).

        Custom models should override this instead of __call__.

        Arguments:
            input_dict (dict): dictionary of input tensors, including "obs",
                "obs_flat", "prev_action", "prev_reward", "is_training"
            state (list): list of state tensors with sizes matching those
                returned by get_initial_state + the batch dimension
            seq_lens (Tensor): 1d tensor holding input sequence lengths

        Returns:
            (outputs, state): The model output tensor of size
                [BATCH, num_outputs]
        """
        raise NotImplementedError

    def value_function(self):
        """Return the value function estimate for the most recent forward pass.

        Returns:
            value estimate tensor of shape [BATCH].
        """
        raise NotImplementedError

    def custom_loss(self, policy_loss, loss_inputs):
        """Override to customize the loss function used to optimize this model.

        This can be used to incorporate self-supervised losses (by defining
        a loss over existing input and output tensors of this model), and
        supervised losses (by defining losses over a variable-sharing copy of
        this model's layers).

        You can find an runnable example in examples/custom_loss.py.

        Arguments:
            policy_loss (Tensor): scalar policy loss from the policy.
            loss_inputs (dict): map of input placeholders for rollout data.

        Returns:
            Scalar tensor for the customized loss for this model.
        """
        return policy_loss

    def metrics(self):
        """Override to return custom metrics from your model.

        The stats will be reported as part of the learner stats, i.e.,
            info:
                learner:
                    model:
                        key1: metric1
                        key2: metric2

        Returns:
            Dict of string keys to scalar tensors.
        """
        return {}

    def __call__(self, input_dict, state=None, seq_lens=None):
        """Call the model with the given input tensors and state.

        This is the method used by RLlib to execute the forward pass. It calls
        forward() internally after unpacking nested observation tensors.

        Custom models should override forward() instead of __call__.

        Arguments:
            input_dict (dict): dictionary of input tensors, including "obs",
                "prev_action", "prev_reward", "is_training"
            state (list): list of state tensors with sizes matching those
                returned by get_initial_state + the batch dimension
            seq_lens (Tensor): 1d tensor holding input sequence lengths

        Returns:
            (outputs, state): The model output tensor of size
                [BATCH, output_spec.size] or a list of tensors corresponding to
                output_spec.shape_list, and a list of state tensors of
                [BATCH, state_size_i].
        """

        restored = input_dict.copy()
        restored["obs"] = restore_original_dimensions(
            input_dict["obs"], self.obs_space, self.framework)
        restored["obs_flat"] = input_dict["obs"]
        res = self.forward(restored, state or [], seq_lens)
        if ((not isinstance(res, list) and not isinstance(res, tuple))
                or len(res) != 2):
            raise ValueError(
                "forward() must return a tuple of (output, state) tensors, "
                "got {}".format(res))
        outputs, state = res

        try:
            shape = outputs.shape
        except AttributeError:
            raise ValueError("Output is not a tensor: {}".format(outputs))
        else:
            if len(shape) != 2 or shape[1] != self.num_outputs:
                raise ValueError(
                    "Expected output shape of [None, {}], got {}".format(
                        self.num_outputs, shape))
        if not isinstance(state, list):
            raise ValueError("State output is not a list: {}".format(state))

        return outputs, state
