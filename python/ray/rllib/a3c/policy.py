from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Policy(object):
    """The policy base class."""
    def __init__(self, ob_space, action_space, name="local", summarize=True):
        pass

    def apply_gradients(self, grads):
        raise NotImplementedError

    def get_weights(self):
        raise NotImplementedError

    def set_weights(self, weights):
        raise NotImplementedError

    def compute_gradients(self, batch):
        raise NotImplementedError

    def compute_action(self, observations):
        """Compute action for a _single_ observation"""
        raise NotImplementedError

    def value(self, ob):
        raise NotImplementedError
