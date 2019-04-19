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
from ray.rllib.utils.explained_variance import explained_variance


class PPOLoss(nn.Module):
    def __init__(self, policy_model, vf_loss_coeff, entropy_coeff, clip_param,
                 vf_clip_param, use_gae):
        nn.Module.__init__(self)
        self.policy_model = policy_model
        self.vf_loss_coeff = vf_loss_coeff
        self.entropy_coeff = entropy_coeff
        self.clip_param = clip_param
        self.vf_clip_param = vf_clip_param
        self.use_gae = use_gae

    def forward(self, observations, value_targets, advantages, actions, logits,
                vf_preds):
        curr_logits, _, values, _ = self.policy_model({
            "obs": observations
        }, [])
        curr_log_probs = F.log_softmax(curr_logits, dim=1)
        curr_probs = F.softmax(curr_logits, dim=1)
        curr_action_log_probs = curr_log_probs.gather(1, actions.view(-1, 1))

        prev_log_probs = F.log_softmax(logits, dim=1)
        prev_probs = F.softmax(logits, dim=1)
        prev_action_log_probs = prev_log_probs.gather(1, actions.view(-1, 1))

        logp_ratio = torch.exp(curr_action_log_probs - prev_action_log_probs)

        curr_entropy = -(curr_log_probs * curr_probs).sum(-1).sum()
        mean_entropy = curr_entropy.sum()

        surrogate_loss = torch.min(
            advantages * logp_ratio,
            advantages * torch.clamp(logp_ratio, 1 - self.clip_param,
                                     1 + self.clip_param))

        mean_policy_loss = (-surrogate_loss).sum()

        if self.use_gae:
            vf_loss1 = F.mse_loss(values.reshape(-1), value_targets)
            vf_clipped = vf_preds + torch.clamp(
                values.reshape(-1) - value_targets, -self.vf_clip_param,
                self.vf_clip_param)
            vf_loss2 = F.mse_loss(vf_clipped, value_targets)
            vf_loss = torch.max(vf_loss1, vf_loss2)
            loss = (-surrogate_loss + self.vf_loss_coeff * vf_loss -
                    self.entropy_coeff * curr_entropy).sum()
        else:
            loss = (-surrogate_loss - self.entropy_coeff * curr_entropy).sum()
        return loss


class PPOTorchPolicyGraph(TorchPolicyGraph):
    def __init__(self, obs_space, action_space, config, existing_inputs=None):
        """
        Arguments:
            obs_space: Environment observation space specification.
            action_space: Environment action space specification.
            config (dict): Configuration values for PPO graph.
            existing_inputs (list): Optional list of tuples that specify the
                placeholders upon which the graph should be built upon.
        """
        config = dict(ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG, **config)
        self.config = config
        _, self.logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        self.model = ModelCatalog.get_torch_model(obs_space, self.logit_dim,
                                                  self.config["model"])
        loss = PPOLoss(self.model, self.config["vf_loss_coeff"],
                       self.config["entropy_coeff"], self.config["clip_param"],
                       self.config["vf_clip_param"], self.config["use_gae"])

        TorchPolicyGraph.__init__(
            self,
            obs_space,
            action_space,
            self.model,
            loss,
            loss_inputs=[
                "obs", "value_targets", "advantages", "actions", "logits",
                "vf_preds"
            ])

    @override(TorchPolicyGraph)
    def extra_action_out(self, model_out):
        return {
            "vf_preds": model_out[2].numpy(),
            "logits": model_out[0].numpy()
        }

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
            next_state = []
            last_r = self._value(sample_batch["new_obs"][-1], *next_state)
        batch = compute_advantages(
            sample_batch,
            last_r,
            self.config["gamma"],
            self.config["lambda"],
            use_gae=self.config["use_gae"])
        return batch

    def _value(self, obs):
        with self.lock:
            obs = torch.from_numpy(obs).float().unsqueeze(0)
            _, _, vf, _ = self.model({"obs": obs}, [])
            return vf.detach().numpy().squeeze()
