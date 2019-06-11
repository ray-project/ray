from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
from ray.rllib.models.model import restore_original_dimensions
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class OutputSpec(namedtuple("OutputSpec", ["size"])):
    """Defines the tensor shape of the model output.

    The model should return a single flat vector of shape [BATCH, size]. In
    the future, more complex output types can be supported via OutputSpec.

    Attributes:
        size (int): size in units of the flat output vector (e.g., 16)
    """
    pass


class ModelV2(object):
    """Defines a Keras-style abstract network model for use with RLlib.

    Custom models should extend either TFModelV2 or TorchModelV2 instead of
    this class directly. Experimental.

    Attributes:
        obs_space (Space): observation space of the target gym env. This
            may have an `original_space` attribute that specifies how to
            unflatten the tensor into a ragged tensor.
        action_space (Space): action space of the target gym env
        output_spec (OutputSpec): defines the output shape of the model
        model_config (dict): config for the model, documented in ModelCatalog
        name (str): name (scope) for the model
        framework (str): either "tf" or "torch"
    """

    def __init__(self, obs_space, action_space, output_spec, model_config,
                 name, framework):
        """Initialize the model.

        This method should create any variables used by the model.
        """

        self.obs_space = obs_space
        self.action_space = action_space
        self.output_spec = output_spec
        self.model_config = model_config
        self.name = name or "default"
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
            (outputs, feature_layer, state): The model output tensor of size
                [BATCH, output_spec.size] or a list of tensors corresponding to
                output_spec.shape_list, a tensor of [BATCH, feature_size], and
                a list of state tensors of [BATCH, state_size_i].
        """
        raise NotImplementedError

    def get_branch_output(self,
                          branch_type,
                          output_spec=None,
                          feature_layer=None,
                          default_impl=None):
        """Get the branch output of the model (e.g., "value" branch).

        This method defines other output heads, for example, the value function
        prediction. The model internally can decide whether to share layers
        with the primary action output head.

        It is important to note that the method outputs are tied to the
        immediately previous call to forward(). This means that after calling
        forward() once, you must retrieve all its branch outputs before calling
        forward() again.

        Arguments:
            branch_type (str): identifier for the branch (e.g., "value")
            output_spec (OutputSpec): defines shape of the branch output
            feature_layer (tensor): if specified, this hints that the branch
                output should be computed using this shared feature layer.
                However custom models are free to ignore this hint.
            default_impl (func): if specified, this function will be called to
                implement the branch output. However custom models are free
                to ignore this and return anything they want.

        Returns:
            tensor: branch output of size [BATCH, output_spec.size]
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
        if self.loss() is not None:
            raise DeprecationWarning(
                "self.loss() is deprecated, use self.custom_loss() instead.")
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

    def variables(self):
        """Returns the list of variables for this model."""
        raise NotImplementedError

    def trainable_variables(self):
        """Returns the list of trainable variables for this model."""
        return self.variables()

    def __call__(self, input_dict, state, seq_lens):
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
            (outputs, feature_layer, state): The model output tensor of size
                [BATCH, output_spec.size] or a list of tensors corresponding to
                output_spec.shape_list, a tensor of [BATCH, feature_size], and
                a list of state tensors of [BATCH, state_size_i].
        """

        if hasattr(self.obs_space, "original_space"):
            restored = input_dict.copy()
            restored["obs"] = restore_original_dimensions(
                input_dict["obs"], self.obs_space, self.framework)
            restored["obs_flat"] = input_dict["obs"]
        outputs, feature_layer, state = self.forward(restored, state, seq_lens)

        try:
            shape = outputs.shape
        except AttributeError:
            raise ValueError("Output is not a tensor: {}".format(outputs))
        else:
            if len(shape) != 2 or shape[1] != self.output_spec.size:
                raise ValueError(
                    "Expected output shape of [None, {}], got {}".format(
                        self.output_spec.size, shape))
        if not hasattr(feature_layer, "shape"):
            raise ValueError(
                "Feature layer is not a tensor: {}".format(feature_layer))
        if not isinstance(state, list):
            raise ValueError("State output is not a list: {}".format(state))

        return outputs, feature_layer, state
