from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch
import torch.nn.functional as F
from torch import nn

import ray
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.evaluation.torch_policy_graph import TorchPolicyGraph


class PGLoss(nn.Module):
    def __init__(self, policy_model):
        nn.Module.__init__(self)
        self.policy_model = policy_model

    def forward(self, observations, actions, advantages):
        logits, values = self.policy_model(observations)
        log_probs = F.log_softmax(logits, dim=1)
        probs = F.softmax(logits, dim=1)
        action_log_probs = log_probs.gather(1, actions.view(-1, 1))
        pi_err = -advantages.dot(action_log_probs.reshape(-1))
        return pi_err


class PGPolicyGraph(TorchPolicyGraph):
    """A simple, non-recurrent PyTorch policy example."""

    def __init__(self, obs_space, action_space, config):
        config = dict(ray.rllib.agents.pg.pg.DEFAULT_CONFIG, **config)
        self.config = config

        # Setup policy
        _, self.logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        self.model = ModelCatalog.get_torch_model(
            obs_space.shape, self.logit_dim, self.config["model"])

        # Setup policy loss
        loss = PGLoss(self.model)

        # Initialize TorchPolicyGraph
        TorchPolicyGraph.__init__(
            self,
            obs_space,
            action_space,
            self.model,
            loss,
            loss_inputs=["obs", "actions", "advantages"])

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        return compute_advantages(
            sample_batch, 0.0, self.config["gamma"], use_gae=False)

