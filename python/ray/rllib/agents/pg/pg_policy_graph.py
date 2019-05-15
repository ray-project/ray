from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.evaluation.postprocessing import compute_advantages, \
    Postprocessing
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


# The basic policy gradients loss
def policy_gradient_loss(postprocessed_batch, action_dist):
    actions = postprocessed_batch[SampleBatch.ACTIONS]
    advantages = postprocessed_batch[Postprocessing.ADVANTAGES]
    return -tf.reduce_mean(action_dist.logp(actions) * advantages)


# This adds the "advantages" column to the sample batch.
def postprocess_advantages(graph,
                           sample_batch,
                           other_agent_batches=None,
                           episode=None):
    return compute_advantages(
        sample_batch, 0.0, graph.config["gamma"], use_gae=False)


def make_optimizer(graph):
    return tf.train.AdamOptimizer(learning_rate=graph.config["lr"])
