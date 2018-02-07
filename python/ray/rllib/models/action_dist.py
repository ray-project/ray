from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import numpy as np
from ray.rllib.utils.reshaper import Reshaper


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
        a0 = self.inputs - tf.reduce_max(self.inputs, reduction_indices=[1],
                                         keep_dims=True)
        ea0 = tf.exp(a0)
        z0 = tf.reduce_sum(ea0, reduction_indices=[1], keep_dims=True)
        p0 = ea0 / z0
        return tf.reduce_sum(p0 * (tf.log(z0) - a0), reduction_indices=[1])

    def kl(self, other):
        a0 = self.inputs - tf.reduce_max(self.inputs, reduction_indices=[1],
                                         keep_dims=True)
        a1 = other.inputs - tf.reduce_max(other.inputs, reduction_indices=[1],
                                          keep_dims=True)
        ea0 = tf.exp(a0)
        ea1 = tf.exp(a1)
        z0 = tf.reduce_sum(ea0, reduction_indices=[1], keep_dims=True)
        z1 = tf.reduce_sum(ea1, reduction_indices=[1], keep_dims=True)
        p0 = ea0 / z0
        return tf.reduce_sum(p0 * (a0 - tf.log(z0) - a1 + tf.log(z1)),
                             reduction_indices=[1])

    def sample(self):
        return tf.multinomial(self.inputs, 1)[0]


class DiagGaussian(ActionDistribution):
    """Action distribution where each vector element is a gaussian.

    The first half of the input vector defines the gaussian means, and the
    second half the gaussian standard deviations.
    """

    def __init__(self, inputs):
        ActionDistribution.__init__(self, inputs)
        mean, log_std = tf.split(inputs, 2, axis=1)
        self.mean = mean
        self.log_std = log_std
        self.std = tf.exp(log_std)

    def logp(self, x):
        return (-0.5 * tf.reduce_sum(tf.square((x - self.mean) / self.std),
                                     reduction_indices=[1]) -
                0.5 * np.log(2.0 * np.pi) * tf.to_float(tf.shape(x)[1]) -
                tf.reduce_sum(self.log_std, reduction_indices=[1]))

    def kl(self, other):
        assert isinstance(other, DiagGaussian)
        return tf.reduce_sum(other.log_std - self.log_std +
                             (tf.square(self.std) +
                              tf.square(self.mean - other.mean)) /
                             (2.0 * tf.square(other.std)) - 0.5,
                             reduction_indices=[1])

    def entropy(self):
        return tf.reduce_sum(self.log_std + .5 * np.log(2.0 * np.pi * np.e),
                             reduction_indices=[1])

    def sample(self):
        return self.mean + self.std * tf.random_normal(tf.shape(self.mean))


class Deterministic(ActionDistribution):
    """Action distribution that returns the input values directly.

    This is similar to DiagGaussian with standard deviation zero.
    """

    def sample(self):
        return self.inputs


class MultiActionDistribution(ActionDistribution):
    """Action distribution that operates for list of actions.

    Args:
        inputs (Tensor list): A list of tensors from which to compute samples.
    """
    def __init__(self, inputs, action_space, child_distributions):
        # you actually have to instantiate the child distributions
        self.reshaper = Reshaper(action_space.spaces)
        split_inputs = self.reshaper.split_tensor(inputs)
        child_list = []
        for i, distribution in enumerate(child_distributions):
            child_list.append(distribution(split_inputs[i]))
        self.child_distributions = child_list

    def logp(self, x):
        """The log-likelihood of the action distribution."""
        split_list = self.reshaper.split_tensor(x)
        for i, distribution in enumerate(self.child_distributions):
            # Remove extra categorical dimension
            if isinstance(distribution, Categorical):
                split_list[i] = tf.squeeze(split_list[i], axis=-1)
        log_list = np.asarray([distribution.logp(split_x) for
                              distribution, split_x in
                               zip(self.child_distributions, split_list)])
        return np.sum(log_list)

    def kl(self, other):
        """The KL-divergence between two action distributions."""
        kl_list = np.asarray([distribution.kl(other_distribution) for
                              distribution, other_distribution in
                              zip(self.child_distributions,
                                  other.child_distributions)])
        return np.sum(kl_list)

    def entropy(self):
        """The entropy of the action distribution."""
        entropy_list = np.array([s.entropy() for s in
                                 self.child_distributions])
        return np.sum(entropy_list)

    def sample(self):
        """Draw a sample from the action distribution."""
        return [[s.sample() for s in self.child_distributions]]
