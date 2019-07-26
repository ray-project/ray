from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.policy.rnn_sequencing import add_time_dimension
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
        """Initialize a TFModelV2.

        Here is an example implementation for a subclass
        ``MyRNNClass(RecurrentTFModelV2)``::

            def __init__(self, *args, **kwargs):
                super(MyModelClass, self).__init__(*args, **kwargs)
                cell_size = 256

                # Define input layers
                input_layer = tf.keras.layers.Input(
                    shape=(None, obs_space.shape[0]))
                state_in_h = tf.keras.layers.Input(shape=(256, ))
                state_in_c = tf.keras.layers.Input(shape=(256, ))
                seq_in = tf.keras.layers.Input(shape=())

                # Send to LSTM cell
                lstm_out, state_h, state_c = tf.keras.layers.LSTM(
                    cell_size, return_sequences=True, return_state=True,
                    name="lstm")(
                        inputs=input_layer,
                        mask=tf.sequence_mask(seq_in),
                        initial_state=[state_in_h, state_in_c])
                output_layer = tf.keras.layers.Dense(...)(lstm_out)

                # Create the RNN model
                self.rnn_model = tf.keras.Model(
                    inputs=[input_layer, seq_in, state_in_h, state_in_c],
                    outputs=[output_layer, state_h, state_c])
                self.register_variables(self.rnn_model.variables)
                self.rnn_model.summary()
        """
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

        Sample implementation for the ``MyRNNClass`` example::

            def forward_rnn(self, inputs, state, seq_lens):
                model_out, h, c = self.rnn_model([inputs, seq_lens] + state)
                return model_out, [h, c]
        """
        raise NotImplementedError("You must implement this for a RNN model")

    def get_initial_state(self):
        """Get the initial recurrent state values for the model.

        Returns:
            list of np.array objects, if any

        Sample implementation for the ``MyRNNClass`` example::

            def get_initial_state(self):
                return [
                    np.zeros(self.cell_size, np.float32),
                    np.zeros(self.cell_size, np.float32),
                ]
        """
        raise NotImplementedError("You must implement this for a RNN model")
