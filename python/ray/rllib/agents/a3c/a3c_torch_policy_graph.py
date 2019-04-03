from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch
import torch.nn.functional as F
from torch import nn

import ray
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.evaluation.postprocessing import compute_advantages, \
    Postprocessing
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.evaluation.torch_policy_graph import TorchPolicyGraph
from ray.rllib.utils.annotations import override


class A3CLoss(nn.Module):
    def __init__(self, dist_class, vf_loss_coeff=0.5, entropy_coeff=0.01):
        nn.Module.__init__(self)
        self.dist_class = dist_class
        self.vf_loss_coeff = vf_loss_coeff
        self.entropy_coeff = entropy_coeff

    def forward(self, policy_model, observations, actions, advantages,
                value_targets):
        logits, _, values, _ = policy_model(
            {SampleBatch.CUR_OBS: observations}, [])
        logits = logits
        values = values
        dist = self.dist_class(logits)
        log_probs = dist.logp(actions)
        if len(log_probs.shape) > 1:
            log_probs = log_probs.sum(-1)
        self.entropy = dist.entropy().mean().cpu()
        self.pi_err = -advantages.dot(log_probs.reshape(-1)).cpu()
        self.value_err = F.mse_loss(values.reshape(-1), value_targets).cpu()
        overall_err = sum([
            self.pi_err,
            self.vf_loss_coeff * self.value_err,
            -self.entropy_coeff * self.entropy,
        ])

        return overall_err


class A3CPostprocessing(object):
    """Adds the VF preds and advantages fields to the trajectory."""

    @override(TorchPolicyGraph)
    def extra_action_out(self, model_out):
        return {SampleBatch.VF_PREDS: model_out[2].cpu().numpy()}

    @override(PolicyGraph)
    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        completed = sample_batch[SampleBatch.DONES][-1]
        if completed:
            last_r = 0.0
        else:
            last_r = self._value(sample_batch[SampleBatch.NEXT_OBS][-1])
        return compute_advantages(sample_batch, last_r, self.config["gamma"],
                                  self.config["lambda"])


class A3CTorchPolicyGraph(A3CPostprocessing, TorchPolicyGraph):
    """A simple, non-recurrent PyTorch policy example."""

    def __init__(self, obs_space, action_space, config):
        config = dict(ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG, **config)
        self.config = config
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"], torch=True)
        model = ModelCatalog.get_torch_model(obs_space, self.logit_dim,
                                             self.config["model"])
        loss = A3CLoss(dist_class, self.config["vf_loss_coeff"],
                       self.config["entropy_coeff"])
        TorchPolicyGraph.__init__(
            self,
            obs_space,
            action_space,
            model,
            loss,
            loss_inputs=[
                SampleBatch.CUR_OBS, SampleBatch.ACTIONS,
                Postprocessing.ADVANTAGES, Postprocessing.VALUE_TARGETS
            ],
            action_distribution_cls=dist_class)

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
            total_norm = nn.utils.clip_grad_norm_(self._model.parameters(),
                                                  self.config["grad_clip"])

            # Note that return values are just references;
            # calling zero_grad will modify the values
            grads = []
            for p in self._model.parameters():
                if p.grad is not None:
                    grads.append(p.grad.data.cpu().numpy())
                else:
                    grads.append(None)

            grad_info = {
                "grad_gnorm": total_norm,
                "policy_entropy": self._loss.entropy.item(),
                "policy_loss": self._loss.pi_err.item(),
                "vf_loss": self._loss.value_err.item()
            }

            return grads, {"stats": grad_info}

    @override(TorchPolicyGraph)
    def optimizer(self):
        return torch.optim.Adam(self._model.parameters(), lr=self.config["lr"])

    def _value(self, obs):
        with self.lock:
            obs = torch.from_numpy(obs).float().unsqueeze(0).to(self.device)
            _, _, vf, _ = self._model({"obs": obs}, [])
            return vf.detach().cpu().numpy().squeeze()
