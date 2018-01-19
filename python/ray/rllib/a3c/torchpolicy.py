from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch
from torch.autograd import Variable

from ray.rllib.a3c.policy import Policy
from threading import Lock


class TorchPolicy(Policy):
    """The policy base class for Torch.

    The model is a separate object than the policy. This could be changed
    in the future."""

    def __init__(self, registry, ob_space, action_space, config,
                 name="local", summarize=True):
        self.registry = registry
        self.local_steps = 0
        self.config = config
        self.summarize = summarize
        self._setup_graph(ob_space, action_space)
        torch.set_num_threads(2)
        self.lock = Lock()

    def apply_gradients(self, grads):
        self.optimizer.zero_grad()
        for g, p in zip(grads, self._model.parameters()):
            p.grad = Variable(torch.from_numpy(g))
        self.optimizer.step()

    def get_weights(self):
        # !! This only returns references to the data.
        return self._model.state_dict()

    def set_weights(self, weights):
        with self.lock:
            self._model.load_state_dict(weights)

    def compute_gradients(self, samples):
        """_backward generates the gradient in each model parameter.
        This is taken out.

        Args:
            samples: SampleBatch of data needed for gradient calculation.

        Return:
            gradients (list of np arrays): List of gradients
            info (dict): Extra information (user-defined)"""
        with self.lock:
            self._backward(samples)
            # Note that return values are just references;
            # calling zero_grad will modify the values
            return [p.grad.data.numpy() for p in self._model.parameters()], {}

    def model_update(self, batch):
        """Implements compute + apply

        TODO(rliaw): Pytorch has nice caching property that doesn't require
        full batch to be passed in. Can exploit that later"""
        with self.lock:
            self._backward(batch)
            self.optimizer.step()

    def _setup_graph(ob_space, action_space):
        raise NotImplementedError

    def _backward(self, batch):
        """Implements the loss function and calculates the gradient.
        Pytorch automatically generates a backward trace for each variable.
        Assumption right now is that variables are moved, so the backward
        trace is lost.

        This function regenerates the backward trace and
        caluclates the gradient."""
        raise NotImplementedError
