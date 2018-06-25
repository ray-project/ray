from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
import tensorflow.contrib.rnn as rnn
import distutils.version

from ray.rllib.models.misc import (conv2d, linear, flatten,
                                   normc_initializer)
from ray.rllib.models.model import Model


def add_time_dimension(padded_inputs, seq_lens):
    """Adds a time dimension to padded inputs.

    Arguments:
        padded_inputs (Tensor): a padded batch of sequences. That is,
            for seq_lens=[1, 2, 2], then inputs=[A, *, B, B, C, C], where
            A, B, C are sequence elements and * denotes padding.
        seq_lens (Tensor): the sequence lengths within the input batch,
            suitable for passing to tf.nn.dynamic_rnn().

    Returns:
        Reshaped tensor of shape [BATCH_SIZE, MAX_SEQ_LEN, ...].
    """

    # Sequence lengths have to be specified for LSTM batch inputs. The
    # input batch must be padded to the max seq length given here. That is,
    # batch_size == len(seq_lens) * max(seq_lens).
    max_seq_len = tf.reduce_max(seq_lens)
    padded_batch_size = tf.shape(padded_inputs)[0]

    # Dynamically reshape the padded batch to introduce a time dimension.
    new_batch_size = padded_batch_size // max_seq_len
    new_shape = (
        [new_batch_size, max_seq_len] +
        padded_inputs.get_shape().as_list()[1:])
    return tf.reshape(padded_inputs, new_shape)


class LSTM(Model):
    """Adds a LSTM cell on top of some other model output.
    
    Important: we assume inputs is a padded batch of sequences denoted by
        self.seq_lens. See add_time_dimension() for more information.
    """

    def _build_layers(self, inputs, num_outputs, options):
        cell_size = options.get("lstm_cell_size", 256)
        use_tf100_api = (distutils.version.LooseVersion(tf.VERSION) >=
                         distutils.version.LooseVersion("1.0.0"))
        last_layer = add_time_dimension(inputs, self.seq_lens)

        # Setup the LSTM cell
        if use_tf100_api:
            lstm = rnn.BasicLSTMCell(cell_size, state_is_tuple=True)
        else:
            lstm = rnn.rnn_cell.BasicLSTMCell(cell_size, state_is_tuple=True)
        self.state_init = [
            np.zeros(lstm.state_size.c, np.float32),
            np.zeros(lstm.state_size.h, np.float32)]

        # Setup LSTM inputs
        c_in = tf.placeholder(tf.float32, [None, lstm.state_size.c], name="c")
        h_in = tf.placeholder(tf.float32, [None, lstm.state_size.h], name="h")
        self.state_in = [c_in, h_in]

        # Setup LSTM outputs
        if use_tf100_api:
            state_in = rnn.LSTMStateTuple(c_in, h_in)
        else:
            state_in = rnn.rnn_cell.LSTMStateTuple(c_in, h_in)
        lstm_out, lstm_state = tf.nn.dynamic_rnn(
            lstm, last_layer, initial_state=state_in,
            sequence_length=self.seq_lens, time_major=False)
        self.state_out = list(lstm_state)

        # Compute outputs
        last_layer = tf.reshape(lstm_out, [-1, cell_size])
        logits = linear(
            last_layer, num_outputs, "action", normc_initializer(0.01))
        return logits, last_layer
