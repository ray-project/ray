from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

import numpy as np
from threading import Lock

try:
    import torch
except ImportError:
    pass  # soft dep

from ray.rllib.evaluation.metrics import LEARNER_STATS_KEY
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.utils.annotations import override


class TorchPolicyGraph(PolicyGraph):
    """Template for a PyTorch policy and loss to use with RLlib.

    This is similar to TFPolicyGraph, but for PyTorch.

    Attributes:
        observation_space (gym.Space): observation space of the policy.
        action_space (gym.Space): action space of the policy.
        lock (Lock): Lock that must be held around PyTorch ops on this graph.
            This is necessary when using the async sampler.
    """

    def __init__(self, observation_space, action_space, model, loss,
                 loss_inputs, action_distribution_cls):
        """Build a policy graph from policy and loss torch modules.

        Note that model will be placed on GPU device if CUDA_VISIBLE_DEVICES
        is set. Only single GPU is supported for now.

        Arguments:
            observation_space (gym.Space): observation space of the policy.
            action_space (gym.Space): action space of the policy.
            model (nn.Module): PyTorch policy module. Given observations as
                input, this module must return a list of outputs where the
                first item is action logits, and the rest can be any value.
            loss (nn.Module): Loss defined as a PyTorch module. The inputs for
                this module are defined by the `loss_inputs` param. This module
                returns a single scalar loss. Note that this module should
                internally be using the model module.
            loss_inputs (list): List of SampleBatch columns that will be
                passed to the loss module's forward() function when computing
                the loss. For example, ["obs", "action", "advantages"].
            action_distribution_cls (ActionDistribution): Class for action
                distribution.
        """
        self.observation_space = observation_space
        self.action_space = action_space
        self.lock = Lock()
        self.device = (torch.device("cuda")
                       if bool(os.environ.get("CUDA_VISIBLE_DEVICES", None))
                       else torch.device("cpu"))
        self._model = model.to(self.device)
        self._loss = loss
        self._loss_inputs = loss_inputs
        self._optimizer = self.optimizer()
        self._action_dist_cls = action_distribution_cls

    @override(PolicyGraph)
    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        **kwargs):
        with self.lock:
            with torch.no_grad():
                ob = torch.from_numpy(np.array(obs_batch)) \
                    .float().to(self.device)
                model_out = self._model({"obs": ob}, state_batches)
                logits, _, vf, state = model_out
                action_dist = self._action_dist_cls(logits)
                actions = action_dist.sample()
                return (actions.cpu().numpy(),
                        [h.cpu().numpy() for h in state],
                        self.extra_action_out(model_out))

    @override(PolicyGraph)
    def learn_on_batch(self, postprocessed_batch):
        with self.lock:
            loss_in = []
            for key in self._loss_inputs:
                loss_in.append(
                    torch.from_numpy(postprocessed_batch[key]).to(self.device))
            loss_out = self._loss(self._model, *loss_in)
            self._optimizer.zero_grad()
            loss_out.backward()

            grad_process_info = self.extra_grad_process()
            self._optimizer.step()

            grad_info = self.extra_grad_info()
            grad_info.update(grad_process_info)
            return {LEARNER_STATS_KEY: grad_info}

    @override(PolicyGraph)
    def compute_gradients(self, postprocessed_batch):
        with self.lock:
            loss_in = []
            for key in self._loss_inputs:
                loss_in.append(
                    torch.from_numpy(postprocessed_batch[key]).to(self.device))
            loss_out = self._loss(self._model, *loss_in)
            self._optimizer.zero_grad()
            loss_out.backward()

            grad_process_info = self.extra_grad_process()

            # Note that return values are just references;
            # calling zero_grad will modify the values
            grads = []
            for p in self._model.parameters():
                if p.grad is not None:
                    grads.append(p.grad.data.cpu().numpy())
                else:
                    grads.append(None)

            grad_info = self.extra_grad_info()
            grad_info.update(grad_process_info)
            return grads, {LEARNER_STATS_KEY: grad_info}

    @override(PolicyGraph)
    def apply_gradients(self, gradients):
        with self.lock:
            for g, p in zip(gradients, self._model.parameters()):
                if g is not None:
                    p.grad = torch.from_numpy(g).to(self.device)
            self._optimizer.step()

    @override(PolicyGraph)
    def get_weights(self):
        with self.lock:
            return {k: v.cpu() for k, v in self._model.state_dict().items()}

    @override(PolicyGraph)
    def set_weights(self, weights):
        with self.lock:
            self._model.load_state_dict(weights)

    @override(PolicyGraph)
    def get_initial_state(self):
        return [s.numpy() for s in self._model.state_init()]

    def extra_grad_process(self):
        """Allow subclass to do extra processing on gradients and
           return processing info."""
        return {}

    def extra_action_out(self, model_out):
        """Returns dict of extra info to include in experience batch.

        Arguments:
            model_out (list): Outputs of the policy model module."""
        return {}

    def extra_grad_info(self):
        """Return dict of extra grad info."""

        return {}

    def optimizer(self):
        """Custom PyTorch optimizer to use."""
        return torch.optim.Adam(self._model.parameters())
