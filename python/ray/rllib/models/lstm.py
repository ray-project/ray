from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""LSTM support for RLlib.

The main trick here is that we add the time dimension at the last moment.
The non-LSTM layers of the model see their inputs as one flat batch. Before
the LSTM cell, we reshape the input to add the expected time dimension. During
postprocessing, we dynamically pad the experience batches so that this
reshaping is possible.

See the add_time_dimension() and chop_into_sequences() functions below for
more info.
"""

import numpy as np
import tensorflow as tf
import tensorflow.contrib.rnn as rnn
import distutils.version

from ray.rllib.models.misc import linear, normc_initializer
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


def chop_into_sequences(episode_ids, feature_columns, state_columns,
                        max_seq_len):
    """Truncate and pad experiences into fixed-length sequences.

    Arguments:
        episode_ids (list): List of episode ids for each step.
        feature_columns (list): List of arrays containing features.
        state_columns (list): List of arrays containing LSTM state values.
        max_seq_len (int): Max length of sequences before truncation.

    Returns:
        f_pad (list): Padded feature columns. These will be of shape
            [NUM_SEQUENCES * MAX_SEQ_LEN, ...].
        s_init (list): Initial states for each sequence, of shape
            [NUM_SEQUENCES, ...].
        seq_lens (list): List of sequence lengths, of shape [NUM_SEQUENCES].

    Examples:
        >>> f_pad, s_init, seq_lens = chop_into_sequences(
                episode_id=[1, 1, 5, 5, 5, 5],
                feature_columns=[[4, 4, 8, 8, 8, 8],
                                 [1, 1, 0, 1, 1, 0]],
                state_columns=[[4, 5, 4, 5, 5, 5]],
                max_seq_len=3)
        >>> print(f_pad)
        [[4, 4, 0, 8, 8, 8, 8, 0, 0],
         [1, 1, 0, 0, 1, 1, 0, 0, 0]]
        >>> print(s_init)
        [[4, 4, 5]]
        >>> print(seq_lens)
        [2, 3, 1]
    """

    prev_id = None
    seq_lens = []
    seq_len = 0
    for eps_id in episode_ids:
        if (prev_id is not None and eps_id != prev_id) or \
                seq_len >= max_seq_len:
            seq_lens.append(seq_len)
            seq_len = 0
        seq_len += 1
        prev_id = eps_id
    if seq_len:
        seq_lens.append(seq_len)
    assert sum(seq_lens) == len(episode_ids)

    # Dynamically shrink max len as needed to optimize memory usage
    max_seq_len = max(seq_lens)

    feature_sequences = []
    for f in feature_columns:
        f = np.array(f)
        f_pad = np.zeros((len(seq_lens) * max_seq_len, ) + np.shape(f)[1:])
        seq_base = 0
        i = 0
        for l in seq_lens:
            for seq_offset in range(l):
                f_pad[seq_base + seq_offset] = f[i]
                i += 1
            seq_base += max_seq_len
        assert i == len(episode_ids), f
        feature_sequences.append(f_pad)

    initial_states = []
    for s in state_columns:
        s = np.array(s)
        s_init = []
        i = 0
        for l in seq_lens:
            s_init.append(s[i])
            i += l
        initial_states.append(np.array(s_init))

    return feature_sequences, initial_states, np.array(seq_lens)


class LSTM(Model):
    """Adds a LSTM cell on top of some other model output.

    Uses a linear layer at the end for output.

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
        if use_tf100_api:
            state_in = rnn.LSTMStateTuple(c_in, h_in)
        else:
            state_in = rnn.rnn_cell.LSTMStateTuple(c_in, h_in)
        lstm_out, lstm_state = tf.nn.dynamic_rnn(
            lstm,
            last_layer,
            initial_state=state_in,
            sequence_length=self.seq_lens,
            time_major=False)
        self.state_out = list(lstm_state)

        # Compute outputs
        last_layer = tf.reshape(lstm_out, [-1, cell_size])
        logits = linear(last_layer, num_outputs, "action",
                        normc_initializer(0.01))
        return logits, last_layer
