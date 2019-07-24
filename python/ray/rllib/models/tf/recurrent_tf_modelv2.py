from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.lstm import add_time_dimension
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class RecurrentTFModelV2(TFModelV2):
    """Helper class to simplify implementing RNN models with TFModelV2.

    Instead of implementing forward(), you can implement forward_rnn() which
    takes batches with the time dimension added already."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TFModelV2.__init__(self, obs_space, action_space, num_outputs,
                           model_config, name)

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        """Adds time dimension to batch before sending inputs to forward_rnn().

        You should implement forward_rnn() in your subclass."""
        output, new_state = self.forward_rnn(
            add_time_dimension(input_dict["obs_flat"], seq_lens), state,
            seq_lens)
        return tf.reshape(output, [-1, self.num_outputs]), new_state

    def forward_rnn(self, inputs, state, seq_lens):
        """Call the model with the given input tensors and state.

        Arguments:
            inputs (dict): observation tensor with shape [B, T, obs_size].
            state (list): list of state tensors, each with shape [B, T, size].
            seq_lens (Tensor): 1d tensor holding input sequence lengths.

        Returns:
            (outputs, new_state): The model output tensor of shape
                [B, T, num_outputs] and the list of new state tensors each with
                shape [B, size].
        """
        raise NotImplementedError("You must implement this for a RNN model")

    def get_initial_state(self):
        raise NotImplementedError("You must implement this for a RNN model")
