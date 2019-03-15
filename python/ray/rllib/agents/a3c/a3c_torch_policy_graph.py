from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch
import torch.nn.functional as F
from torch import nn

import ray
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.torch_policy_graph import TorchPolicyGraph
from ray.rllib.utils.annotations import override


class A3CLoss(nn.Module):
    def __init__(self, policy_model, vf_loss_coeff=0.5, entropy_coeff=0.01):
        nn.Module.__init__(self)
        self.policy_model = policy_model
        self.vf_loss_coeff = vf_loss_coeff
        self.entropy_coeff = entropy_coeff

    def forward(self, observations, actions, advantages, value_targets):
        logits, _, values, _ = self.policy_model({"obs": observations}, [])
        log_probs = F.log_softmax(logits, dim=1)
        probs = F.softmax(logits, dim=1)
        action_log_probs = log_probs.gather(1, actions.view(-1, 1))
        entropy = -(log_probs * probs).sum(-1).sum()
        pi_err = -advantages.dot(action_log_probs.reshape(-1))
        value_err = F.mse_loss(values.reshape(-1), value_targets)
        overall_err = sum([
            pi_err,
            self.vf_loss_coeff * value_err,
            -self.entropy_coeff * entropy,
        ])
        return overall_err


class A3CTorchPolicyGraph(TorchPolicyGraph):
    """A simple, non-recurrent PyTorch policy example."""

    def __init__(self, obs_space, action_space, config):
        config = dict(ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG, **config)
        self.config = config
        _, self.logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        self.model = ModelCatalog.get_torch_model(obs_space, self.logit_dim,
                                                  self.config["model"])
        loss = A3CLoss(self.model, self.config["vf_loss_coeff"],
                       self.config["entropy_coeff"])
        TorchPolicyGraph.__init__(
            self,
            obs_space,
            action_space,
            self.model,
            loss,
            loss_inputs=["obs", "actions", "advantages", "value_targets"])

    @override(TorchPolicyGraph)
    def extra_action_out(self, model_out):
        return {"vf_preds": model_out[2].numpy()}

    @override(TorchPolicyGraph)
    def optimizer(self):
        return torch.optim.Adam(self.model.parameters(), lr=self.config["lr"])

    @override(PolicyGraph)
    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        completed = sample_batch["dones"][-1]
        if completed:
            last_r = 0.0
        else:
            last_r = self._value(sample_batch["new_obs"][-1])
        return compute_advantages(sample_batch, last_r, self.config["gamma"],
                                  self.config["lambda"])

    def _value(self, obs):
        with self.lock:
            obs = torch.from_numpy(obs).float().unsqueeze(0)
            _, _, vf, _ = self.model({"obs": obs}, [])
            return vf.detach().numpy().squeeze()
