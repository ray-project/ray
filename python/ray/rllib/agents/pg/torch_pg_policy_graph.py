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


class PGLoss(nn.Module):
    def __init__(self, policy_model):
        nn.Module.__init__(self)
        self.policy_model = policy_model

    def forward(self, observations, actions, advantages):
        logits, _, values, _ = self.policy_model({"obs": observations}, [])
        log_probs = F.log_softmax(logits, dim=1)
        action_log_probs = log_probs.gather(1, actions.view(-1, 1))
        pi_err = -advantages.dot(action_log_probs.reshape(-1))
        return pi_err


class PGTorchPolicyGraph(TorchPolicyGraph):
    def __init__(self, obs_space, action_space, config):
        config = dict(ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG, **config)
        self.config = config
        _, self.logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        self.model = ModelCatalog.get_torch_model(obs_space, self.logit_dim,
                                                  self.config["model"])
        loss = PGLoss(self.model)

        TorchPolicyGraph.__init__(
            self,
            obs_space,
            action_space,
            self.model,
            loss,
            loss_inputs=["obs", "actions", "advantages"])

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
        return compute_advantages(
            sample_batch, 0.0, self.config["gamma"], use_gae=False)

    def _value(self, obs):
        with self.lock:
            obs = torch.from_numpy(obs).float().unsqueeze(0)
            _, _, vf, _ = self.model({"obs": obs}, [])
            return vf.detach().numpy().squeeze()
