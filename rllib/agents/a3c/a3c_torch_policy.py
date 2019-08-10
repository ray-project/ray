from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch
import torch.nn.functional as F
from torch import nn

import ray
from ray.rllib.evaluation.postprocessing import compute_advantages, \
    Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_template import build_torch_policy


def actor_critic_loss(policy, batch_tensors):
    logits, _ = policy.model({
        SampleBatch.CUR_OBS: batch_tensors[SampleBatch.CUR_OBS]
    })  # TODO(ekl) seq lens shouldn't be None
    values = policy.model.value_function()
    dist = policy.dist_class(logits, policy.config["model"])
    log_probs = dist.logp(batch_tensors[SampleBatch.ACTIONS])
    policy.entropy = dist.entropy().mean()
    policy.pi_err = -batch_tensors[Postprocessing.ADVANTAGES].dot(
        log_probs.reshape(-1))
    policy.value_err = F.mse_loss(
        values.reshape(-1), batch_tensors[Postprocessing.VALUE_TARGETS])
    overall_err = sum([
        policy.pi_err,
        policy.config["vf_loss_coeff"] * policy.value_err,
        -policy.config["entropy_coeff"] * policy.entropy,
    ])
    return overall_err


def loss_and_entropy_stats(policy, batch_tensors):
    return {
        "policy_entropy": policy.entropy.item(),
        "policy_loss": policy.pi_err.item(),
        "vf_loss": policy.value_err.item(),
    }


def add_advantages(policy,
                   sample_batch,
                   other_agent_batches=None,
                   episode=None):
    completed = sample_batch[SampleBatch.DONES][-1]
    if completed:
        last_r = 0.0
    else:
        last_r = policy._value(sample_batch[SampleBatch.NEXT_OBS][-1])
    return compute_advantages(sample_batch, last_r, policy.config["gamma"],
                              policy.config["lambda"])


def model_value_predictions(policy, input_dict, state_batches, model):
    return {SampleBatch.VF_PREDS: model.value_function().cpu().numpy()}


def apply_grad_clipping(policy):
    info = {}
    if policy.config["grad_clip"]:
        total_norm = nn.utils.clip_grad_norm_(policy.model.parameters(),
                                              policy.config["grad_clip"])
        info["grad_gnorm"] = total_norm
    return info


def torch_optimizer(policy, config):
    return torch.optim.Adam(policy.model.parameters(), lr=config["lr"])


class ValueNetworkMixin(object):
    def _value(self, obs):
        with self.lock:
            obs = torch.from_numpy(obs).float().unsqueeze(0).to(self.device)
            _ = self.model({"obs": obs}, [], [1])
            return self.model.value_function().detach().cpu().numpy().squeeze()


A3CTorchPolicy = build_torch_policy(
    name="A3CTorchPolicy",
    get_default_config=lambda: ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG,
    loss_fn=actor_critic_loss,
    stats_fn=loss_and_entropy_stats,
    postprocess_fn=add_advantages,
    extra_action_out_fn=model_value_predictions,
    extra_grad_process_fn=apply_grad_clipping,
    optimizer_fn=torch_optimizer,
    mixins=[ValueNetworkMixin])
