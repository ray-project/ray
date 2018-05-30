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


class ImpalaShadowNet(Model):
    """Vision LSTM network based here:
    https://github.com/openai/universe-starter-agent"""

    # TODO(rliaw): Add LSTM code for other algorithms
    def _init(self, inputs, num_outputs, options):
        use_tf100_api = (distutils.version.LooseVersion(tf.VERSION) >=
                         distutils.version.LooseVersion("1.0.0"))

        num_trajs = options["num_trajs"]
        traj_length = options["traj_length"]
        # (batch_size, ob_space) where, in running time,
        # batch_size is 1 for act, 
        # batch_size is num_trajectories * sample_batch_size for updating
        self.x = _x = inputs
        for i in range(4):
            _x = tf.nn.elu(conv2d(_x, 32, "l{}".format(i + 1), [3, 3], [2, 2]))
        # Introduce a "fake" batch dimension of 1 after flatten so that we can
        # do LSTM over the time dim.
        x = tf.expand_dims(flatten(_x), [0])
        # Transform to standard shape (batch_size, max_time, ob_space)
        # for 'dynamic_rnn' method where batch_size = num_trajs, 
        # max_time = traj_length
        xs = tf.reshape(_x, [num_trajs, traj_length, np.prod(_x.get_shape().as_list()[1:])])

        # set up the cell
        size = 256
        if use_tf100_api:
            lstm = rnn.BasicLSTMCell(size, state_is_tuple=True)
        else:
            lstm = rnn.rnn_cell.BasicLSTMCell(size, state_is_tuple=True)
        step_size = tf.shape(self.x)[:1]

        # common initializers
        c_init = np.zeros((1, lstm.state_size.c), np.float32)
        h_init = np.zeros((1, lstm.state_size.h), np.float32)
        self.state_init = [c_init, h_init]
        # initializer(s)
        c_in = tf.placeholder(tf.float32, [1, lstm.state_size.c])
        h_in = tf.placeholder(tf.float32, [1, lstm.state_size.h])
        c_ins = tf.placeholder(tf.float32, [num_trajs, lstm.state_size.c])
        h_ins = tf.placeholder(tf.float32, [num_trajs, lstm.state_size.h])
        self.state_in = [c_in, h_in]
        self.state_ins = [c_ins, h_ins]

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
            state_ins = rnn.LSTMStateTuple(c_ins, h_ins)
        else:
            state_ins = rnn.rnn_cell.LSTMStateTuple(c_ins, h_ins)
        lstm_outs, lstm_states = tf.nn.dynamic_rnn(lstm, xs,
                                                 initial_state=state_ins,
                                                 sequence_length=num_trajs*[traj_length],
                                                 time_major=False)
        xs = tf.reshape(lstm_outs, [-1, size])
        with tf.variable_scope("lstm-fc", reuse=True):
            self.logits = linear(xs, num_outputs, "action", normc_initializer(0.01))
        self.last_layers = xs
        
        return logit, x
