import logging

import ray
from ray.rllib.agents.dqn.dqn_tf_policy import DQNTFPolicy, QLoss, build_q_losses
from ray.rllib.utils.framework import try_import_tf, try_import_tfp

logger = logging.getLogger(__name__)

tf1, tf, tfv = try_import_tf()
tfp = try_import_tfp()

class CQLQLoss(QLoss):
    def __init__(self,
                 q_t,
                 q_t_selected,
                 q_logits_t_selected,
                 q_tp1_best,
                 q_dist_tp1_best,
                 importance_weights,
                 rewards,
                 done_mask,
                 config,
                 ):

        super().__init__(q_t, q_t_selected, q_logits_t_selected, q_tp1_best,
                         q_dist_tp1_best, importance_weights, rewards, done_mask, config)
        min_q_weight = config["min_q_weight"]

        dataset_expec = tf.reduce_mean(q_t_selected)
        negative_sampling = tf.reduce_mean(tf.reduce_logsumexp(q_t, 1))

        min_q_loss = (negative_sampling - dataset_expec)

        min_q_loss = min_q_loss * min_q_weight
        self.loss = self.loss + min_q_loss
        self.stats["cql_loss"] = min_q_loss


def build_cql_losses(policy, model, dist_class, train_batch):
    return build_q_losses(policy, model, dist_class, train_batch, CQLQLoss)


# Build a child class of `TFPolicy`, given the custom functions defined
# above.
CQLDQNTFPolicy = DQNTFPolicy.with_updates(
    name="CQLDQNTFPolicy",
    get_default_config=lambda: ray.rllib.agents.cql.CQLDQN_DEFAULT_CONFIG,
    loss_fn=build_cql_losses,
)
