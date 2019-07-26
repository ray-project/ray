from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class TorchModelV2(ModelV2):
    """Torch version of ModelV2.

    Note that this class by itself is not a valid model unless you
    implement forward() in a subclass. You'll also probably want to inherit
    from nn.Module in order to implement parameters() automatically."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        """Initialize a TorchModelV2.

        Here is an example implementation for a subclass ``MyModelClass``::

            def __init__(self, *args, **kwargs):
                super(MyModelClass, self).__init__(*args, **kwargs)
                nn.Module.__init__(self)
                self._hidden_layers = nn.Sequential(...)
                self._logits = ...
                self._value_branch = ...
        """
        ModelV2.__init__(
            self,
            obs_space,
            action_space,
            num_outputs,
            model_config,
            name,
            framework="torch")

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

        Sample implementation for the ``MyModelClass`` example::

            def forward(self, input_dict, state, seq_lens):
                features = self._hidden_layers(input_dict["obs"])
                self._value_out = self._value_branch(features)
                return self._logits(features), state
        """
        raise NotImplementedError

    def value_function(self):
        """Return the value function estimate for the most recent forward pass.

        Returns:
            value estimate tensor of shape [BATCH].

        Sample implementation for the ``MyModelClass`` example::

            def value_function(self):
                return self._value_out
        """
        raise NotImplementedError

    def parameters(self):
        """Returns the torch parameters for this model.

        Note that can be satisfied simply by inheriting from nn.Module."""
        raise NotImplementedError(
            "Your subclass should inherit from nn.Module to implement "
            "parameters() automatically.")
