import ray
from ray.rllib.evaluation.postprocessing import Postprocessing, \
    compute_advantages
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


def post_process_advantages(policy,
                            sample_batch,
                            other_agent_batches=None,
                            episode=None):
    """This adds the "advantages" column to the sample train_batch."""
    return compute_advantages(
        sample_batch,
        0.0,
        policy.config["gamma"],
        use_gae=False,
        use_critic=False)


def pg_tf_loss(policy, model, dist_class, train_batch):
    """The basic policy gradients loss."""
    logits, _ = model.from_batch(train_batch)
    action_dist = dist_class(logits, model)
    return -tf.reduce_mean(
        action_dist.logp(train_batch[SampleBatch.ACTIONS]) *
        train_batch[Postprocessing.ADVANTAGES])


PGTFPolicy = build_tf_policy(
    name="PGTFPolicy",
    get_default_config=lambda: ray.rllib.agents.pg.pg.DEFAULT_CONFIG,
    postprocess_fn=post_process_advantages,
    loss_fn=pg_tf_loss)
