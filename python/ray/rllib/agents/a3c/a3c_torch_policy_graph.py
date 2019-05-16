from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch
import torch.nn.functional as F
from torch import nn

import ray
from ray.rllib.evaluation.postprocessing import compute_advantages, \
    Postprocessing
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.evaluation.torch_policy_graph_template import build_torch_policy


def a3c_torch_loss(policy, batch_tensors):
    logits, _, values, _ = policy.model({
        SampleBatch.CUR_OBS: batch_tensors[SampleBatch.CUR_OBS]
    }, [])
    dist = policy.dist_class(logits)
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


def a3c_torch_stats(policy, batch_tensors):
    return {
        "policy_entropy": policy.entropy.item(),
        "policy_loss": policy.pi_err.item(),
        "vf_loss": policy.value_err.item(),
    }


def postprocess_torch_a3c(policy,
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


def a3c_extra_action_out(policy, model_out):
    return {SampleBatch.VF_PREDS: model_out[2].cpu().numpy()}


def a3c_extra_grad_process(policy):
    info = {}
    if policy.config["grad_clip"]:
        total_norm = nn.utils.clip_grad_norm_(policy.model.parameters(),
                                              policy.config["grad_clip"])
        info["grad_gnorm"] = total_norm
    return info


def optimizer(policy):
    return torch.optim.Adam(policy.model.parameters(), lr=policy.config["lr"])


class ValueNetworkMixin(object):
    def _value(self, obs):
        with self.lock:
            obs = torch.from_numpy(obs).float().unsqueeze(0).to(self.device)
            _, _, vf, _ = self.model({"obs": obs}, [])
            return vf.detach().cpu().numpy().squeeze()


A3CTorchPolicyGraph = build_torch_policy(
    name="A3CTorchPolicyGraph",
    get_default_config=lambda: ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG,
    loss_fn=a3c_torch_loss,
    stats_fn=a3c_torch_stats,
    postprocess_fn=postprocess_torch_a3c,
    extra_action_out_fn=a3c_extra_action_out,
    extra_grad_process_fn=a3c_extra_grad_process,
    optimizer_fn=optimizer,
    mixins=[ValueNetworkMixin])
