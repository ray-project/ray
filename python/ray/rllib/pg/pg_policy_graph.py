from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.process_rollout import compute_advantages
from ray.rllib.utils.tf_policy_graph import TFPolicyGraph


class PGPolicyGraph(TFPolicyGraph):

    def __init__(self, obs_space, action_space, registry, config):
        self.config = config

        # setup policy
        self.x = tf.placeholder(tf.float32, shape=[None]+list(obs_space.shape))
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(action_space)
        self.model = ModelCatalog.get_model(
            registry, self.x, self.logit_dim, options=self.config["model"])
        self.dist = dist_class(self.model.outputs)  # logit for each action

        # setup policy loss
        self.ac = ModelCatalog.get_action_placeholder(action_space)
        self.adv = tf.placeholder(tf.float32, [None], name="adv")
        self.loss = -tf.reduce_mean(self.dist.logp(self.ac) * self.adv)

        # initialize TFPolicyGraph
        self.sess = tf.get_default_session()
        self.loss_in = [
            ("obs", self.x),
            ("actions", self.ac),
            ("advantages", self.adv),
        ]
        self.is_training = tf.placeholder_with_default(True, ())
        TFPolicyGraph.__init__(
            self, self.sess, obs_input=self.x,
            action_sampler=self.dist.sample(), loss=self.loss,
            loss_inputs=self.loss_in, is_training=self.is_training)
        self.sess.run(tf.global_variables_initializer())

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        return compute_advantages(
            sample_batch, 0.0, self.config["gamma"], use_gae=False)
