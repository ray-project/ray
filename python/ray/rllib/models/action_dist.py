from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import numpy as np
import gym


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
        self.reshaper = Reshaper(action_space)
        split_inputs = self.reshaper.split_tensor(inputs)
        child_list = []
        for i, distribution in enumerate(child_distributions):
            child_list.append(distribution(split_inputs[i]))
        self.child_distributions = child_list


    def logp(self, x):
        """The log-likelihood of the action distribution."""
        split_list = self.reshaper.split_tensor(x)
        log_list = np.asarray(distribution.logp(split_x) for
                              distribution, split_x in zip(self.child_distributions, split_list))
        # FIXME (ev) do we maybe want these to be lists and no sums?
        return np.sum(log_list)


    def kl(self, other):
        """The KL-divergence between two action distributions."""
        # FIXME (ev) this will probably be a bit tricker
        split_list = self.reshaper.split_tensor(other)
        kl_list = np.asarray(distribution.kl(split_x) for
                              distribution, split_x in zip(self.child_distributions, split_list))
        return np.sum(kl_list)


    def entropy(self):
        """The entropy of the action distribution."""
        entropy_list = np.array([s.entropy() for s in self.child_distributions])
        return np.sum(entropy_list)

    def sample(self):
        """Draw a sample from the action distribution."""
        return np.array([s.sample() for s in self.child_distributions])


# TODO(ev) move this out of here
class Reshaper(object):
    """
    This class keeps track of where in the flattened observation space we should be slicing and what the
    new shapes should be
    """
    # TODO(ev) support discrete action spaces
    def __init__(self, env_space):
        self.shapes = []
        self.slice_positions = []
        self.env_space = env_space
        if isinstance(env_space, list):
            for space in env_space:
                arr_shape = np.asarray(space.shape)
                self.shapes.append(arr_shape)
                if len(self.slice_positions) == 0:
                    self.slice_positions.append(np.product(arr_shape))
                else:
                    self.slice_positions.append(np.product(arr_shape) + self.slice_positions[-1])
        else:
            self.shapes.append(np.asarray(env_space.shape))
            self.slice_positions.append(np.product(env_space.shape))


    def get_flat_shape(self):
        import ipdb; ipdb.set_trace()
        return self.slice_positions[-1]


    def get_slice_lengths(self):
        diffed_list = np.diff(self.slice_positions).tolist()
        diffed_list.insert(0, self.slice_positions[0])
        return np.asarray(diffed_list)


    def get_flat_box(self):
        lows = []
        highs = []
        if isinstance(self.env_space, list):
            for i in range(len(self.env_space)):
                lows += self.env_space[i].low.tolist()
                highs += self.env_space[i].high.tolist()
            return gym.spaces.Box(np.asarray(lows), np.asarray(highs))
        else:
            return gym.spaces.Box(self.env_space.low, self.env_space.high)


    def split_tensor(self, tensor, axis=-1):
        # FIXME (ev) brittle. Should instead use information about distributions to scale appropriately
        slice_rescale = int(tensor.shape.as_list()[axis] / int(np.sum(self.get_slice_lengths())))
        return tf.split(tensor, slice_rescale*self.get_slice_lengths(), axis=axis)


    def split_number(self, number):
        slice_rescale = int(number / int(np.sum(self.get_slice_lengths())))
        return slice_rescale*self.get_slice_lengths()


    def split_agents(self, tensor, axis=-1):
        return tf.split(tensor)