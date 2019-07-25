from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


@DeveloperAPI
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


@DeveloperAPI
def add_time_dimension(padded_inputs, seq_lens):
    """Adds a time dimension to padded inputs.

    Arguments:
        padded_inputs (Tensor): a padded batch of sequences. That is,
            for seq_lens=[1, 2, 2], then inputs=[A, *, B, B, C, C], where
            A, B, C are sequence elements and * denotes padding.
        seq_lens (Tensor): the sequence lengths within the input batch,
            suitable for passing to tf.nn.dynamic_rnn().

    Returns:
        Reshaped tensor of shape [NUM_SEQUENCES, MAX_SEQ_LEN, ...].
    """

    # Sequence lengths have to be specified for LSTM batch inputs. The
    # input batch must be padded to the max seq length given here. That is,
    # batch_size == len(seq_lens) * max(seq_lens)
    padded_batch_size = tf.shape(padded_inputs)[0]
    max_seq_len = padded_batch_size // tf.shape(seq_lens)[0]

    # Dynamically reshape the padded batch to introduce a time dimension.
    new_batch_size = padded_batch_size // max_seq_len
    new_shape = ([new_batch_size, max_seq_len] +
                 padded_inputs.get_shape().as_list()[1:])
    return tf.reshape(padded_inputs, new_shape)
