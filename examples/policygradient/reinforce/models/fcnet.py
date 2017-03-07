import tensorflow as tf
import tensorflow.contrib.slim as slim

import numpy as np

def normc_initializer(std=1.0):
    def _initializer(shape, dtype=None, partition_info=None): #pylint: disable=W0613
        out = np.random.randn(*shape).astype(np.float32)
        out *= std / np.sqrt(np.square(out).sum(axis=0, keepdims=True))
        return tf.constant(out)
    return _initializer

def fc_net(inputs, num_classes=10, logstd=False):
  fc1 = slim.fully_connected(inputs, 128, weights_initializer=normc_initializer(1.0), scope="fc1")
  fc2 = slim.fully_connected(fc1, 128, weights_initializer=normc_initializer(1.0), scope="fc2")
  fc3 = slim.fully_connected(fc2, 128, weights_initializer=normc_initializer(1.0), scope="fc3")
  fc4 = slim.fully_connected(fc3, num_classes, weights_initializer=normc_initializer(0.01), activation_fn=None, scope="fc4")
  if logstd:
    logstd = tf.get_variable(name="logstd", shape=[num_classes], initializer=tf.zeros_initializer)
    return tf.concat(1, [fc4, logstd])
  else:
    return fc4
