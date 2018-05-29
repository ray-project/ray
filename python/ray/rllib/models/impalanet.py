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

    # TODO(rliaw): Add LSTM code for other algorithms
    def _init(self, inputs, num_outputs, options):
        use_tf100_api = (distutils.version.LooseVersion(tf.VERSION) >=
                         distutils.version.LooseVersion("1.0.0"))

        # (batch_size, ob_space) where, in running time,
        # batch_size is 1 for act, 
        # batch_size is num_trajectories * sample_batch_size for updating
        self.x = x = inputs
        for i in range(4):
            x = tf.nn.elu(conv2d(x, 32, "l{}".format(i + 1), [3, 3], [2, 2]))
        # Introduce a "fake" batch dimension of 1 after flatten so that we can
        # do LSTM over the time dim.
        x = tf.expand_dims(flatten(x), [0])
        # Transform to standard shape (batch_size, max_time, ob_space)
        # for 'dynamic_rnn' method
        xs = tf.reshape(x, [-1, options["traj_length"], np.prod(x.get_shape().as_list()[1:])])

        # set up the cell
        size = 256
        if use_tf100_api:
            lstm = rnn.BasicLSTMCell(size, state_is_tuple=True)
        else:
            lstm = rnn.rnn_cell.BasicLSTMCell(size, state_is_tuple=True)
        # shape = (ob_space)
        step_size = tf.shape(self.x)[:1]

        # common initializers
        c_init = np.zeros((1, lstm.state_size.c), np.float32)
        h_init = np.zeros((1, lstm.state_size.h), np.float32)
        self.state_init = [c_init, h_init]
        c_in = tf.placeholder(tf.float32, [1, lstm.state_size.c])
        h_in = tf.placeholder(tf.float32, [1, lstm.state_size.h])
        self.state_in = [c_in, h_in]

        # this branch is for acting
        if use_tf100_api:
            state_in = rnn.LSTMStateTuple(c_in, h_in)
        else:
            state_in = rnn.rnn_cell.LSTMStateTuple(c_in, h_in)
        lstm_out, lstm_state = tf.nn.dynamic_rnn(lstm, x,
                                                 initial_state=state_in,
                                                 sequence_length=step_size,
                                                 time_major=False)
        lstm_c, lstm_h = lstm_state
        x = tf.reshape(lstm_out, [-1, size])
        with tf.variable_scope("lstm-fc"):
            logit = linear(x, num_outputs, "action", normc_initializer(0.01))
        self.state_out = [lstm_c[:1, :], lstm_h[:1, :]]

        # this branch is for updating
        if use_tf100_api:
            state_ins = rnn.LSTMStateTuple(
                tf.tile(c_in, [tf.shape(xs)[0],1]),
                tf.tile(h_in, [tf.shape(xs)[0],1]))
        else:
            state_ins = rnn.rnn_cell.LSTMStateTuple(c_in, h_in)
        lstm_outs, lstm_states = tf.nn.dynamic_rnn(lstm, xs,
                                                 initial_state=state_ins,
                                                 sequence_length=step_size,
                                                 time_major=False)
        #lstm_cs, lstm_hs = lstm_states
        xs = tf.reshape(lstm_outs, [-1, size])
        with tf.variable_scope("lstm-fc", reuse=True):
            logits = linear(xs, num_outputs, "action", normc_initializer(0.01))
            self.logits = tf.reshape(logits, [-1, options["traj_length"], num_outputs])
        #self.state_outs = [lstm_cs, lstm_hs[:1, :]]
        self.xs = tf.reshape(xs, [-1, options["traj_length"], size])
        
        return logit, x
