from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch
from torch import nn

import ray
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.evaluation.postprocessing import compute_advantages, \
    Postprocessing
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.evaluation.torch_policy_graph import TorchPolicyGraph
from ray.rllib.utils.annotations import override


class PGLoss(nn.Module):
    def __init__(self, dist_class):
        nn.Module.__init__(self)
        self.dist_class = dist_class

    def forward(self, policy_model, observations, actions, advantages):
        logits, _, values, _ = policy_model({
            SampleBatch.CUR_OBS: observations
        }, [])
        dist = self.dist_class(logits)
        log_probs = dist.logp(actions)
        self.pi_err = -advantages.dot(log_probs.reshape(-1))
        return self.pi_err


class PGPostprocessing(object):
    """Adds the value func output and advantages field to the trajectory."""

    @override(TorchPolicyGraph)
    def extra_action_out(self, model_out):
        return {SampleBatch.VF_PREDS: model_out[2].cpu().numpy()}

    @override(PolicyGraph)
    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        return compute_advantages(
            sample_batch, 0.0, self.config["gamma"], use_gae=False)


class PGTorchPolicyGraph(PGPostprocessing, TorchPolicyGraph):
    def __init__(self, obs_space, action_space, config):
        config = dict(ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG, **config)
        self.config = config
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"], torch=True)
        model = ModelCatalog.get_torch_model(obs_space, self.logit_dim,
                                             self.config["model"])
        loss = PGLoss(dist_class)

        TorchPolicyGraph.__init__(
            self,
            obs_space,
            action_space,
            model,
            loss,
            loss_inputs=[
                SampleBatch.CUR_OBS, SampleBatch.ACTIONS,
                Postprocessing.ADVANTAGES
            ],
            action_distribution_cls=dist_class)

    @override(TorchPolicyGraph)
    def optimizer(self):
        return torch.optim.Adam(self._model.parameters(), lr=self.config["lr"])

    @override(TorchPolicyGraph)
    def extra_grad_info(self):
        return {"policy_loss": self._loss.pi_err.item()}

    def _value(self, obs):
        with self.lock:
            obs = torch.from_numpy(obs).float().unsqueeze(0).to(self.device)
            _, _, vf, _ = self.model({"obs": obs}, [])
            return vf.detach().cpu().numpy().squeeze()
