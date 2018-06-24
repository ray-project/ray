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


class LSTM(Model):
    """Vision LSTM network based here:
    https://github.com/openai/universe-starter-agent"""

    def _build_layers(self, inputs, num_outputs, options):
        use_tf100_api = (distutils.version.LooseVersion(tf.VERSION) >=
                         distutils.version.LooseVersion("1.0.0"))

        last_layer = inputs  # [BATCH_DIM, last_layer_size...]
        self.seq_lens = tf.placeholder(tf.int32, [None])

        print("Last layer", last_layer)
#        for i in range(4):
#            last_layer = tf.nn.elu(
#                conv2d(last_layer, 32, "l{}".format(i + 1), [3, 3], [2, 2]))
        # Introduce a "fake" batch dimension of 1 after flatten so that we can
        # do LSTM over the time dim.
#        last_layer = tf.expand_dims(flatten(last_layer), [0])

        cell_size = options.get("lstm_cell_size", 256)
        if use_tf100_api:
            lstm = rnn.BasicLSTMCell(cell_size, state_is_tuple=True)
        else:
            lstm = rnn.rnn_cell.BasicLSTMCell(cell_size, state_is_tuple=True)

        c_init = np.zeros(lstm.state_size.c, np.float32)
        h_init = np.zeros(lstm.state_size.h, np.float32)
        self.state_init = [c_init, h_init]
        c_in = tf.placeholder(tf.float32, [1, lstm.state_size.c])
        h_in = tf.placeholder(tf.float32, [1, lstm.state_size.h])
        self.state_in = [c_in, h_in]

        if use_tf100_api:
            state_in = rnn.LSTMStateTuple(c_in, h_in)
        else:
            state_in = rnn.rnn_cell.LSTMStateTuple(c_in, h_in)
        lstm_out, lstm_state = tf.nn.dynamic_rnn(
            lstm, last_layer, initial_state=state_in,
            sequence_length=self.seq_lens, time_major=False)
        lstm_c, lstm_h = lstm_state
        last_layer = tf.reshape(lstm_out, [-1, cell_size])
        logits = linear(
            last_layer, num_outputs, "action", normc_initializer(0.01))
        self.state_out = [lstm_c[:1, :], lstm_h[:1, :]]
        return logits, last_layer
