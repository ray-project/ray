from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.evaluation.postprocessing import compute_advantages, \
    Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_template import build_torch_policy


def pg_torch_loss(policy, batch_tensors):
    logits, _ = policy.model({
        SampleBatch.CUR_OBS: batch_tensors[SampleBatch.CUR_OBS]
    })
    action_dist = policy.dist_class(logits)
    log_probs = action_dist.logp(batch_tensors[SampleBatch.ACTIONS])
    # save the error in the policy object
    policy.pi_err = -batch_tensors[Postprocessing.ADVANTAGES].dot(
        log_probs.reshape(-1))
    return policy.pi_err


def postprocess_advantages(policy,
                           sample_batch,
                           other_agent_batches=None,
                           episode=None):
    return compute_advantages(
        sample_batch, 0.0, policy.config["gamma"], use_gae=False)


def pg_loss_stats(policy, batch_tensors):
    # the error is recorded when computing the loss
    return {"policy_loss": policy.pi_err.item()}


PGTorchPolicy = build_torch_policy(
    name="PGTorchPolicy",
    get_default_config=lambda: ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG,
    loss_fn=pg_torch_loss,
    stats_fn=pg_loss_stats,
    postprocess_fn=postprocess_advantages)
