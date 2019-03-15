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
    def __init__(self, policy_model, vf_loss_coeff=0.5, entropy_coeff=-0.01, 
    	         clip_param, vf_clip_param, vf_loss_coeff, use_gae):
        nn.Module.__init__(self)
        self.policy_model = policy_model
        self.vf_loss_coeff = vf_loss_coeff
        self.entropy_coeff = entropy_coeff
        self.clip_param = clip_param
        self.vf_clip_param = vf_clip_param
        self.vf_loss_coeff = vf_loss_coeff
        self.use_gae = use_gae

    def forward(self, observations, value_target, advantages, actions, logits, vf_preds):
    	def reduce_mean_valid(t):
    		return torch.masked_select(t, valid_mask).sum(-1).sum()

    	dist_cls, _ = ModelCatalog.get_action_dist(action_space, {})
        prev_dist = dist_cls(logits)

        #current probabilities
        curr_logits, _, values, _ = self.policy_model({"obs": observations}, [])
        curr_log_probs = F.log_softmax(curr_logits, dim=1)
        curr_probs = F.softmax(curr_logits, dim=1)
        curr_action_log_probs = cur_log_probs.gather(1, actions.view(-1, 1))

        #previous probabilities
        prev_log_probs = F.log_softmax(logits, dim=1)
        prev_probs = F.softmax(logits, dim=1)
        prev_action_log_probs = prev_log_probs.gather(1, actions.view(-1, 1))

        logp_ratio = torch.exp(curr_action_log_probs - prev_action_log_probs)

        curr_entropy = -(curr_log_probs * curr_probs).sum(-1).sum()
        mean_entropy = reduce_mean_valid(curr_entropy)

        surrogate_loss = torch.min(
        	advantages * logp_ratio,
        	advantages * torch.clamp(logp_ratio, 1 - clip+param, 1 + clip_param))
    	
    	#mean policy loss
    	mean_policy_loss = reduce_mean_valid(-surrogate_loss)
    	
    	#total loss
    	loss = reduce_mean_valid(-surrogate_loss - 
    							  # vf_loss_coeff * vf_loss - 
    							  entropy_coeff * curr_entropy)
        
        return loss

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
        loss = PPOLoss(self.model, self.config["vf_loss_coeff"],
                       self.config["entropy_coeff"], self.config["clip_param"], 
                       self.config["vf_clip_param"], self.config["vf_loss_coeff"], 
                       self.config["use_gae"])

        TorchPolicyGraph.__init__(
            self,
            obs_space, 
            action_space,
            self.model,
            loss,
            loss_inputs=["obs", "value_targets", "advantages", "actions", "logits", "vf_preds"])

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