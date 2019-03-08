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
from ray.rllib.evaluation.torch_policy_graph import TorchPolicyGraph, \
    LearningRateSchedule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.explained_variance import explained_variance

class PPOLoss(nn.Module):
    def __init__(self, policy_model):
        nn.Module.__init__(self)
        self.policy_model = policy_model

    def forward(self, observations, actions, advantages, logits, clip_param = 0.1):
    	dist_cls, _ = ModelCatalog.get_action_dist(action_space, {})
        prev_dist = dist_cls(logits)

        curr_logits, _, values, _ = self.policy_model({"obs": observations}, [])
        curr_log_probs = F.log_softmax(curr_logits, dim=1)
        curr_probs = F.softmax(curr_logits, dim=1)
        curr_action_log_probs = cur_log_probs.gather(1, actions.view(-1, 1))

        prev_log_probs = F.log_softmax(logits, dim=1)
        prev_probs = F.softmax(logits, dim=1)
        prev_action_log_probs = prev_log_probs.gather(1, actions.view(-1, 1))

        logp_ratio = torch.exp(curr_action_log_probs - prev_action_log_probs)

    	#mean entropy
    	mean_entropy = 
    	#mean policy loss
    	mean_policy_loss = x
    	#total loss

        
        curr_entropy = -(log_probs * probs).sum(-1).sum()
        mean_entropy = x
        pi_err = -advantages.dot(action_log_probs.reshape(-1))
        value_err = F.mse_loss(values.reshape(-1), value_targets)
        overall_err = sum([
            pi_err,
            self.vf_loss_coeff * value_err,
            self.entropy_coeff * entropy,
        ])
        return overall_err

class PPOTorchPolicyGraph(TorchPolicyGraph):
    def __init__(self, obs_space, action_space, config, existing_inputs=None):

        """
        Arguments:
            observation_space: Environment observation space specification.
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
        loss = PPOLoss(self.model)

        TorchPolicyGraph.__init__(
            self,
            obs_space, 
            action_space,
            self.model,
            loss,
            loss_inputs=["obs", "actions", "advantages", "logits"])

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
            next_state = []
            for i in range(len(self.model.state_in)):
                next_state.append([sample_batch["state_out_{}".format(i)][-1]])
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