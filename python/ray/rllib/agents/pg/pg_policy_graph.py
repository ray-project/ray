from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.evaluation.postprocessing import compute_advantages, \
    Postprocessing
from ray.rllib.evaluation.dynamic_tf_policy_graph import build_tf_graph
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


# The basic policy gradients loss
def _policy_gradient_loss(graph, postprocessed_batch):
    actions = postprocessed_batch[SampleBatch.ACTIONS]
    advantages = postprocessed_batch[Postprocessing.ADVANTAGES]
    return -tf.reduce_mean(graph.action_dist.logp(actions) * advantages)


# This adds the "advantages" column to the sample batch.
def _postprocess_advantages(graph,
                            sample_batch,
                            other_agent_batches=None,
                            episode=None):
    return compute_advantages(
        sample_batch, 0.0, graph.config["gamma"], use_gae=False)


def _make_optimizer(graph):
    return tf.train.AdamOptimizer(learning_rate=graph.config["lr"])


PGPolicyGraph = build_tf_graph(
    name="PGPolicyGraph",
    get_default_config=lambda: ray.rllib.agents.pg.pg.DEFAULT_CONFIG,
    postprocess_fn=_postprocess_advantages,
    loss_fn=_policy_gradient_loss,
    make_optimizer=_make_optimizer)
