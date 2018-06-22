from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
from threading import Lock

import torch
import torch.nn.functional as F
from torch import nn

from ray.rllib.models.pytorch.misc import var_to_np, convert_batch
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.process_rollout import compute_advantages
from ray.rllib.utils.policy_graph import PolicyGraph


class PGLoss(nn.Module):
    def forward(logits, values, actions, advantages):
        log_probs = F.log_softmax(logits, dim=1)
        probs = F.softmax(logits, dim=1)
        action_log_probs = log_probs.gather(1, actions.view(-1, 1))
        entropy = -(log_probs * probs).sum(-1).sum()
        pi_err = -advantages.dot(action_log_probs.reshape(-1))
        value_err = F.mse_loss(values.reshape(-1), rs)
        overall_err = sum([
            pi_err,
            self.config["vf_loss_coeff"] * value_err,
            self.config["entropy_coeff"] * entropy,
        ])
        return overall_err


class SharedTorchPolicy(TorchPolicyGraph):
    """A simple, non-recurrent PyTorch policy example."""

    def __init__(self, obs_space, action_space, config):
        config = dict(ray.rllib.a3c.a3c.DEFAULT_CONFIG, **config)
        self.config = config
        _, self.logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        model = ModelCatalog.get_torch_model(
            obs_space.shape, self.logit_dim, self.config["model"])
        loss = PGLoss()
        TorchPolicyGraph.__init__(
            self, obs_space, action_space, model, loss,
            model_outputs=["logits", "values"],
            loss_inputs=["logits", "values", "actions", "advantages"])

    def extra_action_out(self, model_out):
        return {"vf_preds": model_out[1]}

    def optimizer(self):
        return = torch.optim.Adam(
            self.model.parameters(), lr=self.config["lr"])

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        completed = sample_batch["dones"][-1]
        if completed:
            last_r = 0.0
        else:
            last_r = self._value(sample_batch["new_obs"][-1])
        return compute_advantages(
            sample_batch, last_r, self.config["gamma"], self.config["lambda"])

    def _value(self, obs):
        with self.lock:
            obs = torch.from_numpy(obs).float().unsqueeze(0)
            res = self.model.hidden_layers(obs)
            res = self.model.value_branch(res)
            res = res.squeeze()
            return var_to_np(res)
