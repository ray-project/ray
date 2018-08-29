from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf


def explained_variance(y, pred):
    _, y_var = tf.nn.moments(y, axes=[0])
    _, diff_var = tf.nn.moments(y - pred, axes=[0])
    return tf.maximum(-1.0, 1 - (diff_var / y_var))
