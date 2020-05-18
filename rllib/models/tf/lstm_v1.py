import numpy as np

from ray.rllib.models.model import Model
from ray.rllib.models.tf.misc import linear, normc_initializer
from ray.rllib.policy.rnn_sequencing import add_time_dimension
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_tf

tf = try_import_tf()


# Deprecated: see as an alternative models/tf/recurrent_net.py
class LSTM(Model):
    """Adds a LSTM cell on top of some other model output.

    Uses a linear layer at the end for output.

    Important: we assume inputs is a padded batch of sequences denoted by
        self.seq_lens. See add_time_dimension() for more information.
    """

    @override(Model)
    def _build_layers_v2(self, input_dict, num_outputs, options):
        # Hard deprecate this class. All Models should use the ModelV2
        # API from here on.
        deprecation_warning("Model->LSTM", "RecurrentNetwork", error=False)

        cell_size = options.get("lstm_cell_size")
        if options.get("lstm_use_prev_action_reward"):
            action_dim = int(
                np.product(
                    input_dict["prev_actions"].get_shape().as_list()[1:]))
            features = tf.concat(
                [
                    input_dict["obs"],
                    tf.reshape(
                        tf.cast(input_dict["prev_actions"], tf.float32),
                        [-1, action_dim]),
                    tf.reshape(input_dict["prev_rewards"], [-1, 1]),
                ],
                axis=1)
        else:
            features = input_dict["obs"]
        last_layer = add_time_dimension(features, self.seq_lens)

        # Setup the LSTM cell
        lstm = tf.nn.rnn_cell.LSTMCell(cell_size, state_is_tuple=True)
        self.state_init = [
            np.zeros(lstm.state_size.c, np.float32),
            np.zeros(lstm.state_size.h, np.float32)
        ]

        # Setup LSTM inputs
        if self.state_in:
            c_in, h_in = self.state_in
        else:
            c_in = tf.placeholder(
                tf.float32, [None, lstm.state_size.c], name="c")
            h_in = tf.placeholder(
                tf.float32, [None, lstm.state_size.h], name="h")
            self.state_in = [c_in, h_in]

        # Setup LSTM outputs
        state_in = tf.nn.rnn_cell.LSTMStateTuple(c_in, h_in)
        lstm_out, lstm_state = tf.nn.dynamic_rnn(
            lstm,
            last_layer,
            initial_state=state_in,
            sequence_length=self.seq_lens,
            time_major=False,
            dtype=tf.float32)

        self.state_out = list(lstm_state)

        # Compute outputs
        last_layer = tf.reshape(lstm_out, [-1, cell_size])
        logits = linear(last_layer, num_outputs, "action",
                        normc_initializer(0.01))
        return logits, last_layer
