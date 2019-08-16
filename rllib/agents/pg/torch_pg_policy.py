from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.evaluation.postprocessing import compute_advantages, \
    Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_template import build_torch_policy


def pg_torch_loss(policy, model, dist_class, batch):
    logits, _ = model.from_batch(batch)
    action_dist = dist_class(logits, model)
    log_probs = action_dist.logp(batch[SampleBatch.ACTIONS])
    # save the error in the policy object
    policy.pi_err = -batch[Postprocessing.ADVANTAGES].dot(
        log_probs.reshape(-1))
    return policy.pi_err


def postprocess_advantages(policy,
                           sample_batch,
                           other_agent_batches=None,
                           episode=None):
    return compute_advantages(
        sample_batch, 0.0, policy.config["gamma"], use_gae=False)


def pg_loss_stats(policy, batch):
    # the error is recorded when computing the loss
    return {"policy_loss": policy.pi_err.item()}


PGTorchPolicy = build_torch_policy(
    name="PGTorchPolicy",
    get_default_config=lambda: ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG,
    loss_fn=pg_torch_loss,
    stats_fn=pg_loss_stats,
    postprocess_fn=postprocess_advantages)
