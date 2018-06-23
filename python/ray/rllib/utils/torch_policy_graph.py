from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
from threading import Lock

import torch
import torch.nn.functional as F

from ray.rllib.models.pytorch.misc import var_to_np
from ray.rllib.utils.policy_graph import PolicyGraph


class TorchPolicyGraph(PolicyGraph):
    """Template for a PyTorch policy and loss to use with RLlib.

    This is similar to TFPolicyGraph, but for PyTorch.

    Attributes:
        observation_space (gym.Space): observation space of the policy.
        action_space (gym.Space): action space of the policy.
        lock (Lock): Lock that must be held around PyTorch ops on this graph.
    """

    def __init__(
            self, observation_space, action_space, model, loss, loss_inputs):
        """Initialize the policy graph.

        Arguments:
            observation_space (gym.Space): observation space of the policy.
            action_space (gym.Space): action space of the policy.
            model (rllib.models.pytorch.Model): PyTorch policy model.
            loss (nn.Module): Loss defined as a PyTorch module.
            loss_inputs (list): List of SampleBatch columns that will be
                passed to the loss module's forward() function when computing
                the loss. For example, ["obs", "action", "advantages"].

        Attributes:
            observation_space (gym.Space): observation space of the policy.
            action_space (gym.Space): action space of the policy.
        """
        self.observation_space = observation_space
        self.action_space = action_space
        self.lock = Lock()
        self._model = model
        self._loss = loss
        self._loss_inputs = loss_inputs
        self._optimizer = self.optimizer()

    def extra_action_out(self, model_out):
        """Returns dict of extra info to include in experience batch.

        Arguments:
            model_out (list): Outputs of the policy model module."""
        return {}

    def optimizer(self):
        """Custom PyTorch optimizer to use."""
        return torch.optim.Adam(self._model.parameters())

    def compute_actions(
            self, obs_batch, state_batches=None, is_training=False):
        if state_batches:
            raise NotImplementedError("Torch RNN support")
        with self.lock:
            ob = torch.from_numpy(np.array(obs_batch)).float()
            model_out = self._model(ob)
            logits = model_out[0]  # assume the first output is the logits
            actions = F.softmax(logits, dim=1).multinomial(1).squeeze(0)
            return var_to_np(actions), [], self.extra_action_out(model_out)

    def compute_gradients(self, postprocessed_batch):
        with self.lock:
            loss_in = []
            for key in self._loss_inputs:
                loss_in.append(torch.from_numpy(postprocessed_batch[key]))
            loss_in = [
                t.float() if isinstance(t, torch.DoubleTensor) else t
                for t in loss_in]
            loss_out = self._loss(*loss_in)
            self._optimizer.zero_grad()
            loss_out.backward()
            # Note that return values are just references;
            # calling zero_grad will modify the values
            return [p.grad.data.numpy() for p in self._model.parameters()], {}

    def apply_gradients(self, gradients):
        with self.lock:
            self._optimizer.zero_grad()
            for g, p in zip(gradients, self._model.parameters()):
                p.grad = torch.from_numpy(g)
            self._optimizer.step()
            return {}

    def get_weights(self):
        with self.lock:
            return self._model.state_dict()

    def set_weights(self, weights):
        with self.lock:
            self._model.load_state_dict(weights)
