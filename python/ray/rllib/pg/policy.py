from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.process_rollout import process_rollout
from ray.rllib.utils.tf_policy import TFPolicy


class PGPolicy(TFPolicy):

    def __init__(self, registry, obs_space, action_space, config):
        self.config = config
        self.registry = registry
        self._setup_graph(obs_space, action_space)
        self._setup_loss(action_space)
        self._setup_gradients()
        self.sess = tf.Session()
        self.loss_in = [
            ("obs", self.x),
            ("actions", self.ac),
            ("advantages", self.adv),
        ]
        self.is_training = tf.placeholder_with_default(True, ())
        TFPolicy.__init__(
            self, self.sess, self.x, self.dist, self.loss, self.loss_in,
            self.is_training)
        self.sess.run(tf.global_variables_initializer())

    def _setup_graph(self, obs_space, action_space):
        self.x = tf.placeholder(tf.float32, shape=[None]+list(obs_space.shape))
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(action_space)
        self.model = ModelCatalog.get_model(
                        self.registry, self.x, self.logit_dim,
                        options=self.config["model"])
        self.action_logits = self.model.outputs  # logit for each action
        self.dist = dist_class(self.action_logits)
        self.sample = self.dist.sample()
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)

    def _setup_loss(self, action_space):
        self.ac = ModelCatalog.get_action_placeholder(action_space)
        self.adv = tf.placeholder(tf.float32, [None], name="adv")

        log_prob = self.dist.logp(self.ac)

        # policy loss
        self.loss = -tf.reduce_mean(log_prob * self.adv)

    def _setup_gradients(self):
        self.grads = tf.gradients(self.loss, self.var_list)
        grads_and_vars = list(zip(self.grads, self.var_list))
        opt = tf.train.AdamOptimizer(self.config["lr"])
        self._apply_gradients = opt.apply_gradients(grads_and_vars)

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        return process_rollout(
            sample_batch, 0.0, self.config["gamma"], use_gae=False)
