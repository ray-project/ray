from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch
import torch.nn as nn
from torch.autograd import Variable
import torch.nn.functional as F

from ray.rllib.a3c.policy import Policy


class TorchPolicy(Policy):
    """The policy base class for Torch.

    The model is a separate object than the policy. This could be changed
    in the future."""

    def __init__(self, ob_space, action_space, name="local", summarize=True):
        self.local_steps = 0
        self.summarize = summarize
        self._setup_graph(ob_space, action_space)
        self.initialize()

    def apply_gradients(self, grads):
        for g, p in zip(grads, self._model.parameters()):
            p.grad = Variable(torch.from_numpy(g))
        self.optimizer.step()

    def get_weights(self):
        ## !! This only returns references to the data.
        return self._model.state_dict()

    def set_weights(self, weights):
        self._model.load_state_dict(weights)

    def compute_gradients(self, batch):
        self._backward(batch)
        # Note that return values are just references;
        # calling zero_grad will modify the values
        return [p.grad.data.numpy() for p in self._model.parameters()], {}

    def model_update(self, batch):
        """ Implements compute + apply """
        # TODO(rliaw): Pytorch has nice
        # caching property that doesn't require
        # full batch to be passed in - can exploit that
        self._backward(batch)
        self.optimizer.step()

    def _setup_graph(ob_space, action_space):
        raise NotImplementedError

    def initialize(self):
        pass

    def _backward(self, batch):
        raise NotImplementedError

    def _evaluate(self, x, actions):
        raise NotImplementedError

    def get_inital_features(self):
        return []
