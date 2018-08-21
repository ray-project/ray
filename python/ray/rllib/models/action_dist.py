from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import numpy as np
import distutils.version

use_tf150_api = (distutils.version.LooseVersion(tf.VERSION) >=
                 distutils.version.LooseVersion("1.5.0"))


class ActionDistribution(object):
    """The policy action distribution of an agent.

    Args:
      inputs (Tensor): The input vector to compute samples from.
    """

    def __init__(self, inputs):
        self.inputs = inputs

    def logp(self, x):
        """The log-likelihood of the action distribution."""
        raise NotImplementedError

    def kl(self, other):
        """The KL-divergence between two action distributions."""
        raise NotImplementedError

    def entropy(self):
        """The entroy of the action distribution."""
        raise NotImplementedError

    def sample(self):
        """Draw a sample from the action distribution."""
        raise NotImplementedError


class Categorical(ActionDistribution):
    """Categorical distribution for discrete action spaces."""

    def logp(self, x):
        return -tf.nn.sparse_softmax_cross_entropy_with_logits(
            logits=self.inputs, labels=x)

    def entropy(self):
        if use_tf150_api:
            a0 = self.inputs - tf.reduce_max(
                self.inputs, reduction_indices=[1], keepdims=True)
        else:
            a0 = self.inputs - tf.reduce_max(
                self.inputs, reduction_indices=[1], keep_dims=True)
        ea0 = tf.exp(a0)
        if use_tf150_api:
            z0 = tf.reduce_sum(ea0, reduction_indices=[1], keepdims=True)
        else:
            z0 = tf.reduce_sum(ea0, reduction_indices=[1], keep_dims=True)
        p0 = ea0 / z0
        return tf.reduce_sum(p0 * (tf.log(z0) - a0), reduction_indices=[1])

    def kl(self, other):
        if use_tf150_api:
            a0 = self.inputs - tf.reduce_max(
                self.inputs, reduction_indices=[1], keepdims=True)
            a1 = other.inputs - tf.reduce_max(
                other.inputs, reduction_indices=[1], keepdims=True)
        else:
            a0 = self.inputs - tf.reduce_max(
                self.inputs, reduction_indices=[1], keep_dims=True)
            a1 = other.inputs - tf.reduce_max(
                other.inputs, reduction_indices=[1], keep_dims=True)
        ea0 = tf.exp(a0)
        ea1 = tf.exp(a1)
        if use_tf150_api:
            z0 = tf.reduce_sum(ea0, reduction_indices=[1], keepdims=True)
            z1 = tf.reduce_sum(ea1, reduction_indices=[1], keepdims=True)
        else:
            z0 = tf.reduce_sum(ea0, reduction_indices=[1], keep_dims=True)
            z1 = tf.reduce_sum(ea1, reduction_indices=[1], keep_dims=True)
        p0 = ea0 / z0
        return tf.reduce_sum(
            p0 * (a0 - tf.log(z0) - a1 + tf.log(z1)), reduction_indices=[1])

    def sample(self):
        return tf.squeeze(tf.multinomial(self.inputs, 1), axis=1)


class DiagGaussian(ActionDistribution):
    """Action distribution where each vector element is a gaussian.

    The first half of the input vector defines the gaussian means, and the
    second half the gaussian standard deviations.
    """

    def __init__(self, inputs, low=None, high=None):
        ActionDistribution.__init__(self, inputs)
        mean, log_std = tf.split(inputs, 2, axis=1)
        self.mean = mean
        self.low = low
        self.high = high

        # Squash to range if specified.
        # TODO(ekl) might make sense to use a beta distribution instead:
        # http://proceedings.mlr.press/v70/chou17a/chou17a.pdf
        if low is not None:
            self.mean = low + tf.sigmoid(self.mean) * (high - low)

        self.log_std = log_std
        self.std = tf.exp(log_std)

    def logp(self, x):
        return (-0.5 * tf.reduce_sum(
            tf.square((x - self.mean) / self.std), reduction_indices=[1]) -
                0.5 * np.log(2.0 * np.pi) * tf.to_float(tf.shape(x)[1]) -
                tf.reduce_sum(self.log_std, reduction_indices=[1]))

    def kl(self, other):
        assert isinstance(other, DiagGaussian)
        return tf.reduce_sum(
            other.log_std - self.log_std +
            (tf.square(self.std) + tf.square(self.mean - other.mean)) /
            (2.0 * tf.square(other.std)) - 0.5,
            reduction_indices=[1])

    def entropy(self):
        return tf.reduce_sum(
            self.log_std + .5 * np.log(2.0 * np.pi * np.e),
            reduction_indices=[1])

    def sample(self):
        out = self.mean + self.std * tf.random_normal(tf.shape(self.mean))
        if self.low is not None:
            out = tf.clip_by_value(out, self.low, self.high)
        return out


class Deterministic(ActionDistribution):
    """Action distribution that returns the input values directly.

    This is similar to DiagGaussian with standard deviation zero.
    """

    def sample(self):
        return self.inputs


def squash_to_range(dist_cls, low, high):
    """Squashes an action distribution to a range in (low, high).

    Arguments:
        dist_cls (class): ActionDistribution class to wrap.
        low (float|array): Scalar value or array of values.
        high (float|array): Scalar value or array of values.
    """

    class SquashToRangeWrapper(dist_cls):
        def __init__(self, inputs):
            dist_cls.__init__(self, inputs, low=low, high=high)

        def logp(self, x):
            return dist_cls.logp(self, x)

        def kl(self, other):
            return dist_cls.kl(self, other)

        def entropy(self):
            return dist_cls.entropy(self)

        def sample(self):
            return dist_cls.sample(self)

    return SquashToRangeWrapper


class MultiActionDistribution(ActionDistribution):
    """Action distribution that operates for list of actions.

    Args:
        inputs (Tensor list): A list of tensors from which to compute samples.
    """

    def __init__(self, inputs, action_space, child_distributions, input_lens):
        self.input_lens = input_lens
        split_inputs = tf.split(inputs, self.input_lens, axis=1)
        child_list = []
        for i, distribution in enumerate(child_distributions):
            child_list.append(distribution(split_inputs[i]))
        self.child_distributions = child_list

    def logp(self, x):
        """The log-likelihood of the action distribution."""
        split_indices = []
        for dist in self.child_distributions:
            if isinstance(dist, Categorical):
                split_indices.append(1)
            else:
                split_indices.append(tf.shape(dist.sample())[1])
        split_list = tf.split(x, split_indices, axis=1)
        for i, distribution in enumerate(self.child_distributions):
            # Remove extra categorical dimension
            if isinstance(distribution, Categorical):
                split_list[i] = tf.cast(
                    tf.squeeze(split_list[i], axis=-1), tf.int32)
        log_list = np.asarray([
            distribution.logp(split_x) for distribution, split_x in zip(
                self.child_distributions, split_list)
        ])
        return np.sum(log_list)

    def kl(self, other):
        """The KL-divergence between two action distributions."""
        kl_list = np.asarray([
            distribution.kl(other_distribution)
            for distribution, other_distribution in zip(
                self.child_distributions, other.child_distributions)
        ])
        return np.sum(kl_list)

    def entropy(self):
        """The entropy of the action distribution."""
        entropy_list = np.array(
            [s.entropy() for s in self.child_distributions])
        return np.sum(entropy_list)

    def sample(self):
        """Draw a sample from the action distribution."""
        return [[s.sample() for s in self.child_distributions]]
