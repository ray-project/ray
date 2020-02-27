import ray
from ray.rllib.agents.pg.pg_tf_policy import post_process_advantages
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


def pg_torch_loss(policy, model, dist_class, train_batch):
    """The basic policy gradients loss."""
    logits, _ = model.from_batch(train_batch)
    action_dist = dist_class(logits, model)
    log_probs = action_dist.logp(train_batch[SampleBatch.ACTIONS])
    # Save the error in the policy object.
    # policy.pi_err = -train_batch[Postprocessing.ADVANTAGES].dot(
    # log_probs.reshape(-1)) / len(log_probs)
    policy.pi_err = -torch.mean(
        log_probs * train_batch[Postprocessing.ADVANTAGES])
    return policy.pi_err


def pg_loss_stats(policy, train_batch):
    """ The error is recorded when computing the loss."""
    return {"policy_loss": policy.pi_err.item()}


PGTorchPolicy = build_torch_policy(
    name="PGTorchPolicy",
    get_default_config=lambda: ray.rllib.agents.pg.pg.DEFAULT_CONFIG,
    loss_fn=pg_torch_loss,
    stats_fn=pg_loss_stats,
    postprocess_fn=post_process_advantages)
