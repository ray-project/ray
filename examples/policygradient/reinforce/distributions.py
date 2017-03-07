import tensorflow as tf
import numpy as np

class Categorical(object):

  def __init__(self, logits):
    self.logits = logits

  def logp(self, x):
    return -tf.nn.sparse_softmax_cross_entropy_with_logits(self.logits, x)

  def entropy(self):
    a0 = self.logits - tf.reduce_max(self.logits, reduction_indices=[1], keep_dims=True)
    ea0 = tf.exp(a0)
    z0 = tf.reduce_sum(ea0, reduction_indices=[1], keep_dims=True)
    p0 = ea0 / z0
    return tf.reduce_sum(p0 * (tf.log(z0) - a0), reduction_indices=[1])

  def kl(self, other):
    a0 = self.logits - tf.reduce_max(self.logits, reduction_indices=[1], keep_dims=True)
    a1 = other.logits - tf.reduce_max(other.logits, reduction_indices=[1], keep_dims=True)
    ea0 = tf.exp(a0)
    ea1 = tf.exp(a1)
    z0 = tf.reduce_sum(ea0, reduction_indices=[1], keep_dims=True)
    z1 = tf.reduce_sum(ea1, reduction_indices=[1], keep_dims=True)
    p0 = ea0 / z0
    return tf.reduce_sum(p0 * (a0 - tf.log(z0) - a1 + tf.log(z1)), reduction_indices=[1])

  def sample(self):
    return tf.multinomial(self.logits, 1)

class DiagGaussian(object):

    def __init__(self, flat):
        self.flat = flat
        mean, logstd = tf.split(1, 2, flat)
        self.mean = mean
        self.logstd = logstd
        self.std = tf.exp(logstd)

    def logp(self, x):
        return - 0.5 * tf.reduce_sum(tf.square((x - self.mean) / self.std), reduction_indices=[1]) \
               - 0.5 * np.log(2.0 * np.pi) * tf.to_float(tf.shape(x)[1]) \
               - tf.reduce_sum(self.logstd, reduction_indices=[1])

    def kl(self, other):
        assert isinstance(other, DiagGaussian)
        return tf.reduce_sum(other.logstd - self.logstd + (tf.square(self.std) + tf.square(self.mean - other.mean)) / (2.0 * tf.square(other.std)) - 0.5, reduction_indices=[1])

    def entropy(self):
        return tf.reduce_sum(self.logstd + .5 * np.log(2.0 * np.pi * np.e), reduction_indices=[1])

    def sample(self):
        return self.mean + self.std * tf.random_normal(tf.shape(self.mean))
